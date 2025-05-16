import os
import logging
from flask import Flask, jsonify, request as flask_request
from flask_cors import CORS
import telebot
from telebot import types
from dotenv import load_dotenv
import threading
import time
import random
import re
import hmac
import hashlib
from urllib.parse import unquote, parse_qs, quote
from datetime import datetime as dt, timezone, timedelta
import json

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base, backref
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import text

# Pytoniq imports
from pytoniq import LiteBalancer
import asyncio


load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_NAME = os.environ.get("MINI_APP_NAME", "case") 
MINI_APP_URL = os.environ.get("MINI_APP_URL", f"https://t.me/caseKviBot/{MINI_APP_NAME}")
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if not DATABASE_URL:
    logger.error("DATABASE_URL не установлен!")
    exit("DATABASE_URL is not set. Exiting.")

engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=False)
    username = Column(String, nullable=True, index=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    ton_balance = Column(Float, default=0.0, nullable=False)
    star_balance = Column(Integer, default=0, nullable=False)
    referral_code = Column(String, unique=True, index=True, nullable=True)
    referred_by_id = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    referral_earnings_pending = Column(Float, default=0.0, nullable=False)
    total_won_ton = Column(Float, default=0.0, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    inventory = relationship("InventoryItem", back_populates="owner", cascade="all, delete-orphan")
    pending_deposits = relationship("PendingDeposit", back_populates="owner")

    referrer = relationship("User", remote_side=[id], foreign_keys=[referred_by_id], back_populates="referrals_made", uselist=False)
    referrals_made = relationship("User", back_populates="referrer")

class NFT(Base):
    __tablename__ = "nfts"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True, index=True, nullable=False)
    image_filename = Column(String, nullable=True)
    floor_price = Column(Float, default=0.0, nullable=False)
    __table_args__ = (UniqueConstraint('name', name='uq_nft_name'),)

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    nft_id = Column(Integer, ForeignKey("nfts.id"), nullable=False)
    current_value = Column(Float, nullable=False)
    upgrade_multiplier = Column(Float, default=1.0, nullable=False)
    obtained_at = Column(DateTime(timezone=True), server_default=func.now())
    variant = Column(String, nullable=True) 

    owner = relationship("User", back_populates="inventory")
    nft = relationship("NFT")

class PendingDeposit(Base):
    __tablename__ = "pending_deposits"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    original_amount_ton = Column(Float, nullable=False)
    unique_identifier_nano_ton = Column(BigInteger, nullable=False)
    final_amount_nano_ton = Column(BigInteger, nullable=False, index=True)
    expected_comment = Column(String, nullable=False, default="cpd7r07ud3s")
    status = Column(String, default="pending", index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)
    owner = relationship("User", back_populates="pending_deposits")

class PromoCode(Base):
    __tablename__ = "promo_codes"
    id = Column(Integer, primary_key=True, index=True)
    code_text = Column(String, unique=True, index=True, nullable=False)
    activations_left = Column(Integer, nullable=False, default=0)
    ton_amount = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

Base.metadata.create_all(bind=engine)

def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png"
    name_str_replaced_hyphens = name_str.replace('-', '_')
    if name_str_replaced_hyphens in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']:
         return name_str_replaced_hyphens + '.png'
    cleaned_name = name_str.replace('&', 'and')
    cleaned_name = cleaned_name.replace("'", "")
    cleaned_name = re.sub(r'\s+', '-', cleaned_name)
    return cleaned_name + '.png'

UPDATED_FLOOR_PRICES = {
    'Plush Pepe': 1200.0, 'Neko Helmet': 15.0, 'Sharp Tongue': 17.0, "Durov's Cap": 251.0,
    'Voodoo Doll': 9.4, 'Vintage Cigar': 19.7, 'Astral Shard': 50.0, 'Scared Cat': 22.0,
    'Swiss Watch': 18.6, 'Perfume Bottle': 38.3, 'Precious Peach': 100.0, 'Toy Bear': 16.3,
    'Genie Lamp': 19.3, 'Loot Bag': 25.0, 'Kissed Frog': 14.8, 'Electric Skull': 10.9,
    'Diamond Ring': 8.06, 'Mini Oscar': 40.5,
    'Party Sparkler': 2.0, 'Homemade Cake': 2.0, 'Cookie Heart': 1.8, 'Jack-in-the-box': 2.0,
    'Skull Flower': 3.4, 'Lol Pop': 1.4, 'Hynpo Lollipop': 1.4, 'Desk Calendar': 1.4,
    'B-Day Candle': 1.4, 'Record Player': 4.0, 'Jelly Bunny': 3.6, 'Tama Gadget': 4.0, 
    'Snow Globe': 4.0, 'Eternal Rose': 11.0, 'Love Potion': 5.4, 'Top Hat': 6.0
}

# Reverted to original fixed prices, only floor prices of items are updated.
cases_data_backend = [
    {
        'id': 'lolpop', 'name': 'Lol Pop Stash', 'imageFilename': generate_image_filename_from_name('Lol Pop'),
        'priceTON': 1.5, # Original fixed price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.001 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': UPDATED_FLOOR_PRICES['Neko Helmet'], 'probability': 0.005 },
            { 'name': 'Party Sparkler', 'imageFilename': generate_image_filename_from_name('Party Sparkler'), 'floorPrice': UPDATED_FLOOR_PRICES['Party Sparkler'], 'probability': 0.07 },
            { 'name': 'Homemade Cake', 'imageFilename': generate_image_filename_from_name('Homemade Cake'), 'floorPrice': UPDATED_FLOOR_PRICES['Homemade Cake'], 'probability': 0.07 },
            { 'name': 'Cookie Heart', 'imageFilename': generate_image_filename_from_name('Cookie Heart'), 'floorPrice': UPDATED_FLOOR_PRICES['Cookie Heart'], 'probability': 0.07 },
            { 'name': 'Jack-in-the-box', 'imageFilename': generate_image_filename_from_name('Jack-in-the-box'), 'floorPrice': UPDATED_FLOOR_PRICES['Jack-in-the-box'], 'probability': 0.06 },
            { 'name': 'Skull Flower', 'imageFilename': generate_image_filename_from_name('Skull Flower'), 'floorPrice': UPDATED_FLOOR_PRICES['Skull Flower'], 'probability': 0.023 },
            { 'name': 'Lol Pop', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'floorPrice': UPDATED_FLOOR_PRICES['Lol Pop'], 'probability': 0.25 },
            { 'name': 'Hynpo Lollipop', 'imageFilename': generate_image_filename_from_name('Hynpo Lollipop'), 'floorPrice': UPDATED_FLOOR_PRICES['Hynpo Lollipop'], 'probability': 0.25 },
            { 'name': 'Desk Calendar', 'imageFilename': generate_image_filename_from_name('Desk Calendar'), 'floorPrice': UPDATED_FLOOR_PRICES['Desk Calendar'], 'probability': 0.10 },
            { 'name': 'B-Day Candle', 'imageFilename': generate_image_filename_from_name('B-Day Candle'), 'floorPrice': UPDATED_FLOOR_PRICES['B-Day Candle'], 'probability': 0.101 }, # ensure sum is 1
        ]
    },
    {
        'id': 'recordplayer', 'name': 'Record Player Vault', 'imageFilename': generate_image_filename_from_name('Record Player'),
        'priceTON': 6.0, # Original fixed price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.0012 },
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': UPDATED_FLOOR_PRICES['Record Player'], 'probability': 0.40 },
            { 'name': 'Lol Pop', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'floorPrice': UPDATED_FLOOR_PRICES['Lol Pop'], 'probability': 0.10 },
            { 'name': 'Hynpo Lollipop', 'imageFilename': generate_image_filename_from_name('Hynpo Lollipop'), 'floorPrice': UPDATED_FLOOR_PRICES['Hynpo Lollipop'], 'probability': 0.10 },
            { 'name': 'Party Sparkler', 'imageFilename': generate_image_filename_from_name('Party Sparkler'), 'floorPrice': UPDATED_FLOOR_PRICES['Party Sparkler'], 'probability': 0.10 },
            { 'name': 'Skull Flower', 'imageFilename': generate_image_filename_from_name('Skull Flower'), 'floorPrice': UPDATED_FLOOR_PRICES['Skull Flower'], 'probability': 0.10 },
            { 'name': 'Jelly Bunny', 'imageFilename': generate_image_filename_from_name('Jelly Bunny'), 'floorPrice': UPDATED_FLOOR_PRICES['Jelly Bunny'], 'probability': 0.0988 },
            { 'name': 'Tama Gadget', 'imageFilename': generate_image_filename_from_name('Tama Gadget'), 'floorPrice': UPDATED_FLOOR_PRICES['Tama Gadget'], 'probability': 0.05 },
            { 'name': 'Snow Globe', 'imageFilename': generate_image_filename_from_name('Snow Globe'), 'floorPrice': UPDATED_FLOOR_PRICES['Snow Globe'], 'probability': 0.05 },
        ]
    },
    {
        'id': 'swisswatch', 'name': 'Swiss Watch Box', 'imageFilename': generate_image_filename_from_name('Swiss Watch'),
        'priceTON': 10.0, # Original fixed price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.0015 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': UPDATED_FLOOR_PRICES['Swiss Watch'], 'probability': 0.08 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': UPDATED_FLOOR_PRICES['Neko Helmet'], 'probability': 0.10 },
            { 'name': 'Eternal Rose', 'imageFilename': generate_image_filename_from_name('Eternal Rose'), 'floorPrice': UPDATED_FLOOR_PRICES['Eternal Rose'], 'probability': 0.05 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': UPDATED_FLOOR_PRICES['Electric Skull'], 'probability': 0.03 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': UPDATED_FLOOR_PRICES['Diamond Ring'], 'probability': 0.0395 },
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': UPDATED_FLOOR_PRICES['Record Player'], 'probability': 0.20 },
            { 'name': 'Love Potion', 'imageFilename': generate_image_filename_from_name('Love Potion'), 'floorPrice': UPDATED_FLOOR_PRICES['Love Potion'], 'probability': 0.20 },
            { 'name': 'Top Hat', 'imageFilename': generate_image_filename_from_name('Top Hat'), 'floorPrice': UPDATED_FLOOR_PRICES['Top Hat'], 'probability': 0.15 },
            { 'name': 'Voodoo Doll', 'imageFilename': generate_image_filename_from_name('Voodoo Doll'), 'floorPrice': UPDATED_FLOOR_PRICES['Voodoo Doll'], 'probability': 0.149 },
        ]
    },
    {
        'id': 'perfumebottle', 'name': 'Perfume Chest', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'),
        'priceTON': 20.0, # Original fixed price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.0018 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': UPDATED_FLOOR_PRICES['Perfume Bottle'], 'probability': 0.08 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': UPDATED_FLOOR_PRICES['Sharp Tongue'], 'probability': 0.12 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': UPDATED_FLOOR_PRICES['Loot Bag'], 'probability': 0.09946 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': UPDATED_FLOOR_PRICES['Swiss Watch'], 'probability': 0.15 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': UPDATED_FLOOR_PRICES['Neko Helmet'], 'probability': 0.15 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': UPDATED_FLOOR_PRICES['Genie Lamp'], 'probability': 0.15 },
            { 'name': 'Kissed Frog', 'imageFilename': generate_image_filename_from_name('Kissed Frog'), 'floorPrice': UPDATED_FLOOR_PRICES['Kissed Frog'], 'probability': 0.10 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': UPDATED_FLOOR_PRICES['Electric Skull'], 'probability': 0.07 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': UPDATED_FLOOR_PRICES['Diamond Ring'], 'probability': 0.07874 },
        ]
    },
    {
        'id': 'vintagecigar', 'name': 'Vintage Cigar Safe', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'),
        'priceTON': 40.0, # Original fixed price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.002 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': UPDATED_FLOOR_PRICES['Perfume Bottle'], 'probability': 0.2994 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': UPDATED_FLOOR_PRICES['Vintage Cigar'], 'probability': 0.12 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': UPDATED_FLOOR_PRICES['Swiss Watch'], 'probability': 0.12 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': UPDATED_FLOOR_PRICES['Neko Helmet'], 'probability': 0.10 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': UPDATED_FLOOR_PRICES['Sharp Tongue'], 'probability': 0.10 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': UPDATED_FLOOR_PRICES['Genie Lamp'], 'probability': 0.08 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': UPDATED_FLOOR_PRICES['Mini Oscar'], 'probability': 0.08 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': UPDATED_FLOOR_PRICES['Scared Cat'], 'probability': 0.05 },
            { 'name': 'Toy Bear', 'imageFilename': generate_image_filename_from_name('Toy Bear'), 'floorPrice': UPDATED_FLOOR_PRICES['Toy Bear'], 'probability': 0.0486 },
        ]
    },
    {
        'id': 'astralshard', 'name': 'Astral Shard Relic', 'imageFilename': generate_image_filename_from_name('Astral Shard'),
        'priceTON': 100.0, # Original fixed price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.0025 },
            { 'name': 'Durov\'s Cap', 'imageFilename': generate_image_filename_from_name('Durov\'s Cap'), 'floorPrice': UPDATED_FLOOR_PRICES['Durov\'s Cap'], 'probability': 0.09925 },
            { 'name': 'Astral Shard', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'floorPrice': UPDATED_FLOOR_PRICES['Astral Shard'], 'probability': 0.10 },
            { 'name': 'Precious Peach', 'imageFilename': generate_image_filename_from_name('Precious Peach'), 'floorPrice': UPDATED_FLOOR_PRICES['Precious Peach'], 'probability': 0.10 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': UPDATED_FLOOR_PRICES['Vintage Cigar'], 'probability': 0.12 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': UPDATED_FLOOR_PRICES['Perfume Bottle'], 'probability': 0.12 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': UPDATED_FLOOR_PRICES['Swiss Watch'], 'probability': 0.10 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': UPDATED_FLOOR_PRICES['Neko Helmet'], 'probability': 0.08 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': UPDATED_FLOOR_PRICES['Mini Oscar'], 'probability': 0.10 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': UPDATED_FLOOR_PRICES['Scared Cat'], 'probability': 0.08 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': UPDATED_FLOOR_PRICES['Loot Bag'], 'probability': 0.05 },
            { 'name': 'Toy Bear', 'imageFilename': generate_image_filename_from_name('Toy Bear'), 'floorPrice': UPDATED_FLOOR_PRICES['Toy Bear'], 'probability': 0.04825 },
        ]
    },
    {
        'id': 'plushpepe', 'name': 'Plush Pepe Hoard', 'imageFilename': generate_image_filename_from_name('Plush Pepe'),
        'priceTON': 200.0, # Original fixed price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.15 },
            { 'name': 'Durov\'s Cap', 'imageFilename': generate_image_filename_from_name('Durov\'s Cap'), 'floorPrice': UPDATED_FLOOR_PRICES['Durov\'s Cap'], 'probability': 0.25 },
            { 'name': 'Astral Shard', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'floorPrice': UPDATED_FLOOR_PRICES['Astral Shard'], 'probability': 0.60 },
        ]
    },
    {
        'id': 'black', 'name': 'BLACK Singularity',
        'isBackgroundCase': True, 'bgImageFilename': 'image-1.png', 'overlayPrizeName': 'Neko Helmet',
        'priceTON': 30.0, # Manually set price
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': UPDATED_FLOOR_PRICES['Plush Pepe'], 'probability': 0.001 },
            { 'name': 'Durov\'s Cap', 'imageFilename': generate_image_filename_from_name('Durov\'s Cap'), 'floorPrice': UPDATED_FLOOR_PRICES['Durov\'s Cap'], 'probability': 0.01 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': UPDATED_FLOOR_PRICES['Perfume Bottle'], 'probability': 0.05 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': UPDATED_FLOOR_PRICES['Mini Oscar'], 'probability': 0.04 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': UPDATED_FLOOR_PRICES['Scared Cat'], 'probability': 0.06 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': UPDATED_FLOOR_PRICES['Vintage Cigar'], 'probability': 0.07 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': UPDATED_FLOOR_PRICES['Loot Bag'], 'probability': 0.07 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': UPDATED_FLOOR_PRICES['Sharp Tongue'], 'probability': 0.08 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': UPDATED_FLOOR_PRICES['Genie Lamp'], 'probability': 0.08 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': UPDATED_FLOOR_PRICES['Swiss Watch'], 'probability': 0.10 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': UPDATED_FLOOR_PRICES['Neko Helmet'], 'probability': 0.15 },
            { 'name': 'Kissed Frog', 'imageFilename': generate_image_filename_from_name('Kissed Frog'), 'floorPrice': UPDATED_FLOOR_PRICES['Kissed Frog'], 'probability': 0.10 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': UPDATED_FLOOR_PRICES['Electric Skull'], 'probability': 0.09 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': UPDATED_FLOOR_PRICES['Diamond Ring'], 'probability': 0.089},
        ]
    },
]

# Log actual RTPs for these fixed prices
for case_info in cases_data_backend:
    ev = 0
    for p_info in case_info['prizes']:
        # For BLACK Singularity, the EV uses the multiplied value of prizes
        prize_val_for_ev = p_info['floorPrice']
        if case_info['id'] == 'black':
            prize_val_for_ev *= 2.5 # The 2-3x multiplier (using 2.5x example)
        ev += prize_val_for_ev * p_info['probability']
    
    actual_rtp = (ev / case_info['priceTON']) * 100 if case_info['priceTON'] > 0 else float('inf')
    logger.info(f"Case (fixed price): {case_info['name']}, Price: {case_info['priceTON']:.2f} TON, EV: {ev:.2f}, Actual RTP: {actual_rtp:.2f}%")


if not cases_data_backend:
    logger.critical("Массив cases_data_backend ПУСТ! Заполни его!")

def populate_initial_data():
    db = SessionLocal()
    try:
        existing_nft_names = {name[0] for name in db.query(NFT.name).all()}
        nfts_to_add_or_update = []
        
        for prize_name_key, floor_price_val in UPDATED_FLOOR_PRICES.items():
            if prize_name_key not in existing_nft_names:
                nfts_to_add_or_update.append(NFT(
                    name=prize_name_key,
                    image_filename=generate_image_filename_from_name(prize_name_key),
                    floor_price=floor_price_val
                ))
                existing_nft_names.add(prize_name_key) # Add to set to prevent re-checking below
            else:
                # If it exists, check if floor price needs update
                nft_in_db = db.query(NFT).filter(NFT.name == prize_name_key).first()
                if nft_in_db and nft_in_db.floor_price != floor_price_val:
                    nft_in_db.floor_price = floor_price_val
                    # No need to add to nfts_to_add_or_update, will be committed with session
        
        if nfts_to_add_or_update: # Only add new NFTs
            db.add_all(nfts_to_add_or_update)
        
        db.commit() # Commit all changes (new NFTs and updated prices)
        logger.info(f"Populated/Updated NFT data. New: {len(nfts_to_add_or_update)}.")
        
        durov_code = db.query(PromoCode).filter(PromoCode.code_text == 'durov').first()
        if not durov_code:
            db.add(PromoCode(code_text='durov', activations_left=10, ton_amount=5.0)); db.commit()
            logger.info("Promocode 'durov' seeded.")
    except Exception as e:
        db.rollback(); logger.error(f"Ошибка populate_initial_data: {e}", exc_info=True)
    finally: db.close()

populate_initial_data()

# --- Constants for Deposit ---
DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR"
DEPOSIT_COMMENT = "cpd7r07ud3s"
PENDING_DEPOSIT_EXPIRY_MINUTES = 30

# --- Flask Приложение ---
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["https://vasiliy-katsyka.github.io"]}}) # Adjust origins if needed

if not BOT_TOKEN:
    logger.error("Токен бота (BOT_TOKEN) не найден!")
    exit("BOT_TOKEN is not set.")
bot = telebot.TeleBot(BOT_TOKEN)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def validate_init_data(init_data_str: str, bot_token: str) -> dict | None:
    try:
        if not init_data_str: return None
        parsed_data = dict(parse_qs(init_data_str))
        if not all(k in parsed_data for k in ['hash', 'user', 'auth_date']): return None
        
        hash_received = parsed_data.pop('hash')[0]
        auth_date_ts = int(parsed_data['auth_date'][0])
        if (int(dt.now(timezone.utc).timestamp()) - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS: return None

        data_check_string = "\n".join(f"{k}={parsed_data[k][0]}" for k in sorted(parsed_data.keys()))
        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if calculated_hash_hex == hash_received:
            user_info_dict = json.loads(unquote(parsed_data['user'][0]))
            if 'id' not in user_info_dict: return None
            return { "id": int(user_info_dict["id"]), **user_info_dict }
        return None
    except Exception as e:
        logger.error(f"initData validation error: {e}", exc_info=True); return None

# --- API Endpoints ---
@app.route('/')
def index_route(): return "Pusik Gifts App is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]
    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: # Should be created by /start, but as a fallback:
        user = User(id=user_id, username=auth_user_data.get("username"), first_name=auth_user_data.get("first_name"), last_name=auth_user_data.get("last_name"), referral_code=f"ref_{user_id}_{random.randint(1000,9999)}")
        db.add(user); db.commit(); db.refresh(user)

    inventory_data = [{
        "id": item.id, "name": item.nft.name, "imageFilename": item.nft.image_filename,
        "floorPrice": item.nft.floor_price, # Base floor price
        "currentValue": item.current_value, # Value, possibly altered by variant/upgrades
        "upgradeMultiplier": item.upgrade_multiplier, "variant": item.variant, # Pass variant
        "obtained_at": item.obtained_at.isoformat() if item.obtained_at else None
    } for item in user.inventory]
    invited_friends_count = db.query(User).filter(User.referred_by_id == user_id).count()
    return jsonify({
        "id": user.id, "username": user.username, "first_name": user.first_name, "last_name": user.last_name,
        "tonBalance": user.ton_balance, "starBalance": user.star_balance, "inventory": inventory_data,
        "referralCode": user.referral_code, "referralEarningsPending": user.referral_earnings_pending,
        "total_won_ton": user.total_won_ton, "invited_friends_count": invited_friends_count
    })

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]
    data = flask_request.get_json(); case_id = data.get('case_id')
    if not case_id: return jsonify({"error": "case_id required"}), 400

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404

    target_case = next((c for c in cases_data_backend if c['id'] == case_id), None)
    if not target_case: return jsonify({"error": "Case not found"}), 404

    case_cost_ton = target_case['priceTON']
    if user.ton_balance < case_cost_ton:
        return jsonify({"error": f"Not enough TON. Need {case_cost_ton:.2f}, have {user.ton_balance:.2f}"}), 400

    prizes = target_case['prizes']
    if not prizes: return jsonify({"error": "No prizes in case"}), 500

    # Weighted random choice
    rand_val = random.random()
    current_prob_sum = 0
    chosen_prize_info = None
    for prize_info in prizes:
        current_prob_sum += prize_info['probability']
        if rand_val <= current_prob_sum:
            chosen_prize_info = prize_info
            break
    if not chosen_prize_info: chosen_prize_info = random.choice(prizes) # Fallback

    user.ton_balance -= case_cost_ton
    
    db_nft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
    if not db_nft: # Should not happen if populate_initial_data ran
        logger.error(f"CRITICAL: NFT '{chosen_prize_info['name']}' not in DB during case open!"); 
        # Give back TON and error out
        user.ton_balance += case_cost_ton; db.commit()
        return jsonify({"error": "Internal error: Prize NFT data missing"}), 500

    item_variant = "black_singularity" if target_case['id'] == 'black' else None
    
    # Calculate actual value for this item instance
    actual_item_value = db_nft.floor_price
    if item_variant == "black_singularity":
        actual_item_value *= 2.5 # or random.uniform(2.0, 3.0)

    user.total_won_ton += actual_item_value # Add the *actual value won* to total

    new_item = InventoryItem(user_id=user.id, nft_id=db_nft.id, current_value=actual_item_value, variant=item_variant)
    db.add(new_item); db.commit(); db.refresh(new_item)

    return jsonify({
        "status": "success",
        "won_prize": {
            "id": new_item.id, "name": db_nft.name, "imageFilename": db_nft.image_filename,
            "floorPrice": db_nft.floor_price, # Base floor price for display
            "currentValue": new_item.current_value, # Actual value of this instance
            "variant": new_item.variant
        },
        "new_balance_ton": user.ton_balance,
    })

# Other API endpoints (upgrade, convert, sell, deposit, verify, leaderboard, referrals, promocode, finalize_withdrawal)
# ... (These would be largely the same as your previous version, just ensure they use `current_value` from InventoryItem correctly)
# ... Make sure `finalize_withdrawal_api` correctly deducts `item_to_remove.current_value` from `total_won_ton`.

@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]
    data = flask_request.get_json()
    inventory_item_id = data.get('inventory_item_id'); multiplier_str = data.get('multiplier_str')
    if not all([inventory_item_id, multiplier_str]): return jsonify({"error": "Missing params"}), 400
    try: multiplier = float(multiplier_str); inventory_item_id = int(inventory_item_id)
    except ValueError: return jsonify({"error": "Invalid data format"}), 400

    upgrade_chances_map = {1.5: 50, 2.0: 35, 3.0: 25, 5.0: 15, 10.0: 8, 20.0: 3}
    if multiplier not in upgrade_chances_map: return jsonify({"error": "Invalid multiplier"}), 400
    
    db = next(get_db())
    item_to_upgrade = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()
    if not item_to_upgrade: return jsonify({"error": "Item not found"}), 404
    user = db.query(User).filter(User.id == user_id).first()

    if random.uniform(0, 100) < upgrade_chances_map[multiplier]:
        original_value = item_to_upgrade.current_value
        new_value = round(original_value * multiplier, 2)
        value_increase = new_value - original_value
        item_to_upgrade.current_value = new_value
        item_to_upgrade.upgrade_multiplier *= multiplier
        if user: user.total_won_ton += value_increase
        db.commit()
        return jsonify({"status": "success", "message": f"Upgrade successful! New value: {new_value:.2f} TON", "item": {"id": item_to_upgrade.id, "currentValue": new_value, "name": item_to_upgrade.nft.name, "upgradeMultiplier": item_to_upgrade.upgrade_multiplier, "variant": item_to_upgrade.variant }})
    else:
        item_name_lost = item_to_upgrade.nft.name; lost_value = item_to_upgrade.current_value
        if user: user.total_won_ton -= lost_value
        db.delete(item_to_upgrade); db.commit()
        return jsonify({"status": "failed", "message": f"Upgrade failed! You lost {item_name_lost}.", "item_lost": True})

@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; data = flask_request.get_json(); inventory_item_id = data.get('inventory_item_id')
    if not inventory_item_id: return jsonify({"error": "ID required"}), 400
    try: inventory_item_id = int(inventory_item_id)
    except ValueError: return jsonify({"error": "Invalid ID"}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == user_id).first()
    item_to_convert = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()
    if not user or not item_to_convert: return jsonify({"error": "User/item not found"}), 404
    conversion_value = item_to_convert.current_value; user.ton_balance += conversion_value
    db.delete(item_to_convert); db.commit()
    return jsonify({"status": "success", "message": f"{item_to_convert.nft.name} converted for {conversion_value:.2f} TON.", "new_balance_ton": user.ton_balance})

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; db = next(get_db()); user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404
    if not user.inventory: return jsonify({"status": "no_items", "message": "Inventory empty."})
    total_sell_value = sum(item.current_value for item in user.inventory)
    user.ton_balance += total_sell_value
    for item in list(user.inventory): db.delete(item)
    db.commit()
    return jsonify({"status": "success", "message": f"All sold for {total_sell_value:.2f} TON.", "new_balance_ton": user.ton_balance})

@app.route('/api/initiate_deposit', methods=['POST'])
def initiate_deposit_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; data = flask_request.get_json(); amount_str = data.get('amount')
    if amount_str is None: return jsonify({"error": "Amount required"}), 400
    try: original_amount_ton = float(amount_str)
    except ValueError: return jsonify({"error": "Invalid amount"}), 400
    if not (0 < original_amount_ton <= 10000): return jsonify({"error": "Amount out of range"}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404
    unique_nano = random.randint(10000, 999999); final_nano = int(original_amount_ton * 1e9) + unique_nano
    if db.query(PendingDeposit).filter(PendingDeposit.user_id == user_id, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first():
        return jsonify({"error": "Active deposit exists."}), 409
    pending = PendingDeposit(user_id=user_id, original_amount_ton=original_amount_ton, unique_identifier_nano_ton=unique_nano, final_amount_nano_ton=final_nano, expected_comment=DEPOSIT_COMMENT, expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
    db.add(pending); db.commit(); db.refresh(pending)
    display_amount = f"{final_nano / 1e9:.9f}".rstrip('0').rstrip('.')
    return jsonify({"status": "success", "pending_deposit_id": pending.id, "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW, "amount_to_send": display_amount, "final_amount_nano_ton": final_nano, "comment": DEPOSIT_COMMENT, "expires_at": pending.expires_at.isoformat()})

async def check_blockchain_for_deposit(pending_deposit: PendingDeposit, db_session):
    provider = None
    try:
        provider = LiteBalancer.from_mainnet_config(trust_level=2); await provider.start_up()
        txs = await provider.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=30)
        for tx in txs:
            if tx.in_msg and tx.in_msg.is_internal and \
               tx.in_msg.info.value_coins == pending_deposit.final_amount_nano_ton and \
               tx.now > int((pending_deposit.created_at - timedelta(minutes=5)).timestamp()):
                comment_slice = tx.in_msg.body.begin_parse()
                if comment_slice.remaining_bits >= 32 and comment_slice.load_uint(32) == 0:
                    try:
                        if comment_slice.load_snake_string() == pending_deposit.expected_comment:
                            user = db_session.query(User).filter(User.id == pending_deposit.user_id).first()
                            if not user: pending_deposit.status = 'failed'; db_session.commit(); return {"status": "error", "message": "User vanished"}
                            user.ton_balance += pending_deposit.original_amount_ton
                            if user.referred_by_id:
                                referrer = db_session.query(User).filter(User.id == user.referred_by_id).first()
                                if referrer: referrer.referral_earnings_pending += round(pending_deposit.original_amount_ton * 0.10, 2)
                            pending_deposit.status = 'completed'; db_session.commit()
                            return {"status": "success", "message": "Deposit confirmed!", "new_balance_ton": user.ton_balance}
                    except: pass # Comment parse failed or mismatch
        if pending_deposit.expires_at <= dt.now(timezone.utc) and pending_deposit.status == 'pending':
            pending_deposit.status = 'expired'; db_session.commit()
            return {"status": "expired", "message": "Deposit expired."}
        return {"status": "pending", "message": "Transaction not confirmed."}
    except Exception as e: logger.error(f"Blockchain check error: {e}", exc_info=True); return {"status": "error", "message": "Error checking transaction."}
    finally:
        if provider: await provider.close_all()

@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; data = flask_request.get_json(); pending_id = data.get('pending_deposit_id')
    if not pending_id: return jsonify({"error": "ID required"}), 400
    db = next(get_db()); pending = db.query(PendingDeposit).filter(PendingDeposit.id == pending_id, PendingDeposit.user_id == user_id).first()
    if not pending: return jsonify({"error": "Deposit not found"}), 404
    if pending.status == 'completed': user = db.query(User).filter(User.id == user_id).first(); return jsonify({"status": "success", "message": "Already confirmed.", "new_balance_ton": user.ton_balance if user else 0})
    if pending.status == 'expired' or pending.expires_at <= dt.now(timezone.utc):
        if pending.status == 'pending': pending.status = 'expired'; db.commit()
        return jsonify({"status": "expired", "message": "Deposit expired."}), 400
    loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    try: result = loop.run_until_complete(check_blockchain_for_deposit(pending, db))
    finally: loop.close()
    return jsonify(result)

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db()); leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
    return jsonify([{"rank": r+1, "name": u.first_name or u.username or f"User_{str(u.id)[:6]}", "avatarChar": (u.first_name or u.username or "U")[0].upper(), "income": u.total_won_ton, "user_id": u.id} for r, u in enumerate(leaders)])

@app.route('/api/withdraw_referral_earnings', methods=['POST'])
def withdraw_referral_earnings_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; db = next(get_db()); user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404
    if user.referral_earnings_pending > 0:
        withdrawn = user.referral_earnings_pending; user.ton_balance += withdrawn; user.referral_earnings_pending = 0.0; db.commit()
        return jsonify({"status": "success", "message": f"{withdrawn:.2f} TON withdrawn.", "new_balance_ton": user.ton_balance, "new_referral_earnings_pending": 0.0})
    return jsonify({"status": "no_earnings", "message": "No earnings."})

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; data = flask_request.get_json(); code_txt = data.get('promocode_text', "").strip()
    if not code_txt: return jsonify({"status": "error", "message": "Code empty."}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == user_id).first(); promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).first()
    if not user: return jsonify({"status": "error", "message": "User not found."}), 404
    if not promo: return jsonify({"status": "error", "message": "Invalid code."}), 404
    if promo.activations_left <= 0: return jsonify({"status": "error", "message": "Code expired."}), 400
    promo.activations_left -= 1; user.ton_balance += promo.ton_amount
    try: db.commit(); return jsonify({"status": "success", "message": f"Code '{code_txt}' redeemed! +{promo.ton_amount:.2f} TON.", "new_balance_ton": user.ton_balance})
    except SQLAlchemyError: db.rollback(); return jsonify({"status": "error", "message": "DB error."}), 500

@app.route('/api/finalize_withdrawal/<int:inventory_item_id>', methods=['POST'])
def finalize_withdrawal_api(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; db = next(get_db())
    item = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()
    if not item: return jsonify({"status": "error", "message": "Item not found."}), 404
    name = item.nft.name; value = item.current_value
    user = db.query(User).filter(User.id == user_id).first()
    if user: user.total_won_ton = max(0, user.total_won_ton - value) # Deduct actual current value
    db.delete(item)
    try: db.commit(); return jsonify({"status": "success", "message": f"Withdrawal of '{name}' confirmed."})
    except SQLAlchemyError: db.rollback(); return jsonify({"status": "error", "message": "DB error."}), 500

# --- Telegram Bot Commands ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"/start from {message.chat.id} ({message.from_user.username}) with text: '{message.text}'")
    db = next(get_db())
    user_id = message.chat.id
    tg_user_obj = message.from_user
    user = db.query(User).filter(User.id == user_id).first()
    created_now = False

    if not user:
        created_now = True
        user = User(id=user_id, username=tg_user_obj.username, first_name=tg_user_obj.first_name, last_name=tg_user_obj.last_name, referral_code=f"ref_{user_id}_{random.randint(1000,9999)}")
        db.add(user)
        logger.info(f"New user created: {user_id}")
    
    # Process referral from start parameter
    try:
        command_parts = message.text.split(' ')
        if len(command_parts) > 1 and command_parts[1].startswith('startapp='):
            start_param_value = command_parts[1].split('=')[1]
            if start_param_value.startswith('ref_'):
                referrer_code = start_param_value
                if (created_now or not user.referred_by_id): # Apply only if new or no previous referrer
                    referrer = db.query(User).filter(User.referral_code == referrer_code).first()
                    if referrer and referrer.id != user.id:
                        user.referred_by_id = referrer.id
                        logger.info(f"User {user_id} referred by {referrer.id} via deep link.")
                        try: bot.send_message(referrer.id, f"🎉 {user.first_name or user.username or user.id} joined via your Mini App link!")
                        except Exception as e_notify: logger.warning(f"Failed to notify referrer {referrer.id}: {e_notify}")
                    elif not referrer:
                         logger.warning(f"Referral code {referrer_code} not found for user {user_id}.")
                    elif referrer.id == user.id:
                         logger.info(f"User {user_id} attempted to refer self.")
    except Exception as e:
        logger.error(f"Error processing start parameter for {user_id}: {e}")

    # Update user info if changed
    updated_fields = False
    if user.username != tg_user_obj.username: user.username = tg_user_obj.username; updated_fields = True
    if user.first_name != tg_user_obj.first_name: user.first_name = tg_user_obj.first_name; updated_fields = True
    if user.last_name != tg_user_obj.last_name: user.last_name = tg_user_obj.last_name; updated_fields = True
    
    if created_now or updated_fields:
        try: db.commit()
        except Exception as e_commit: db.rollback(); logger.error(f"Error saving user {user_id}: {e_commit}")

    # Point 5: Use MINI_APP_NAME for the button URL
    # The MINI_APP_URL should be the direct link to the Mini App, not just the bot.
    # The Mini App itself handles the "startapp" parameter when launched.
    # The bot's role here is just to provide the button to launch the app.
    
    # The MINI_APP_URL should be set like: https://t.me/YourBotUsername/YourMiniAppName
    # If MINI_APP_NAME is "case", and bot username is "caseKviBot", then
    # MINI_APP_URL becomes "https://t.me/caseKviBot/case"
    
    current_mini_app_url_for_button = f"https://t.me/{bot.get_me().username}/{MINI_APP_NAME}"
    # If your MINI_APP_URL env var is already this full URL, you can use it directly.
    # current_mini_app_url_for_button = MINI_APP_URL 


    markup = types.InlineKeyboardMarkup()
    web_app_info = types.WebAppInfo(url=current_mini_app_url_for_button) # Url to launch the Mini App directly
    app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app_info)
    markup.add(app_button)
    bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже!", reply_markup=markup)


@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.reply_to(message, "Нажмите /start, чтобы открыть Pusik Gifts.")

# --- Polling ---
bot_polling_started = False; bot_polling_thread = None
def run_bot_polling():
    global bot_polling_started
    if bot_polling_started: return
    bot_polling_started = True; logger.info("Starting bot polling...")
    for i in range(3):
        try: bot.remove_webhook(); logger.info("Webhook removed."); break
        except Exception as e: logger.warning(f"Webhook removal attempt {i+1} failed: {e}"); time.sleep(2)
    while bot_polling_started:
        try:
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code in [401, 409]: bot_polling_started = False; logger.error(f"Critical API error {e.error_code}. Polling stopped."); break
            logger.error(f"Telegram API Exception: {e}", exc_info=True); time.sleep(30)
        except Exception as e: logger.error(f"Critical polling error: {e}", exc_info=True); time.sleep(60)
        if not bot_polling_started: break # Check before explicit sleep
        time.sleep(15) # If infinity_polling exits without critical error
    logger.info("Bot polling loop terminated.")


if __name__ == '__main__':
    if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        bot_polling_thread = threading.Thread(target=run_bot_polling, daemon=True)
        bot_polling_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
