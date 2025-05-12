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
import hmac
import hashlib
from urllib.parse import unquote, parse_qs
from datetime import datetime as dt, timezone, timedelta 
import json 

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text # Импортируй text

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_URL = os.environ.get("MINI_APP_URL", "https://default_mini_app_url.io") 
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



# --- SQLAlchemy Настройка ---
if not DATABASE_URL:
    logger.error("DATABASE_URL не установлен в переменных окружения!")
    exit("DATABASE_URL is not set. Exiting.")

engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Модели Базы Данных ---
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
    owner = relationship("User", back_populates="inventory")
    nft = relationship("NFT")

Base.metadata.create_all(bind=engine)

# --- Функция генерации имени файла ---
def generate_image_filename_from_name(name_str: str) -> str: 
    if not name_str: return 'placeholder.png'
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Kissed Frog Happy Pepe": return "Kissed-Frog-Happy-Pepe.png"
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png" 
    cleaned_name = name_str.replace(' ', '-').replace('&', 'and').replace("'", "")
    return cleaned_name + '.png'

# --- Данные кейсов ---
# 🔴🔴🔴 ВСТАВЬ СЮДА СВОЙ ПОЛНЫЙ МАССИВ cases_data_backend 🔴🔴🔴
# ОБЯЗАТЕЛЬНО ЗАПОЛНИ ЭТОТ МАССИВ, ИНАЧЕ ЛОГИКА НЕ БУДЕТ РАБОТАТЬ!
cases_data_backend = [
    { 
        'id': 'lolpop', 'name': 'Lol Pop Stash', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'priceTON': 0.5,
        'prizes': [
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.01 },
            { 'name': 'Party Sparkler', 'imageFilename': generate_image_filename_from_name('Party Sparkler'), 'floorPrice': 1, 'probability': 0.10 },
            { 'name': 'B-Day Candle', 'imageFilename': generate_image_filename_from_name('B-Day Candle'), 'floorPrice': 0.7, 'probability': 0.10 },
            { 'name': 'Homemade Cake', 'imageFilename': generate_image_filename_from_name('Homemade Cake'), 'floorPrice': 1, 'probability': 0.09 },
            { 'name': 'Lol Pop', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'floorPrice': 0.7, 'probability': 0.20 },
            { 'name': 'Hynpo Lollipop', 'imageFilename': generate_image_filename_from_name('Hynpo Lollipop'), 'floorPrice': 0.7, 'probability': 0.20 },
            { 'name': 'Desk Calendar', 'imageFilename': generate_image_filename_from_name('Desk Calendar'), 'floorPrice': 0.7, 'probability': 0.10 },
            { 'name': 'Cookie Heart', 'imageFilename': generate_image_filename_from_name('Cookie Heart'), 'floorPrice': 0.9, 'probability': 0.10 },
            { 'name': 'Jack-in-the-box', 'imageFilename': generate_image_filename_from_name('Jack-in-the-box'), 'floorPrice': 1, 'probability': 0.08 },
            { 'name': 'Skull Flower', 'imageFilename': generate_image_filename_from_name('Skull Flower'), 'floorPrice': 1.7, 'probability': 0.02 },
        ]
    },
    { 
        'id': 'recordplayer', 'name': 'Record Player Vault', 'imageFilename': generate_image_filename_from_name('Record Player'), 'priceTON': 3,
        'prizes': [
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': 2, 'probability': 0.50 },
            { 'name': 'Lol Pop', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'floorPrice': 0.7, 'probability': 0.08 },
            { 'name': 'Hynpo Lollipop', 'imageFilename': generate_image_filename_from_name('Hynpo Lollipop'), 'floorPrice': 0.7, 'probability': 0.08 },
            { 'name': 'Party Sparkler', 'imageFilename': generate_image_filename_from_name('Party Sparkler'), 'floorPrice': 1, 'probability': 0.08 },
            { 'name': 'Skull Flower', 'imageFilename': generate_image_filename_from_name('Skull Flower'), 'floorPrice': 1.7, 'probability': 0.08 },
            { 'name': 'Jelly Bunny', 'imageFilename': generate_image_filename_from_name('Jelly Bunny'), 'floorPrice': 1.8, 'probability': 0.08 },
            { 'name': 'Tama Gadget', 'imageFilename': generate_image_filename_from_name('Tama Gadget'), 'floorPrice': 2, 'probability': 0.05 },
            { 'name': 'Snow Globe', 'imageFilename': generate_image_filename_from_name('Snow Globe'), 'floorPrice': 2, 'probability': 0.05 },
        ]
    },
    { 
        'id': 'swisswatch', 'name': 'Swiss Watch Box', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'priceTON': 5,
        'prizes': [
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.10 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.20 },
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': 2, 'probability': 0.10 },
            { 'name': 'Love Potion', 'imageFilename': generate_image_filename_from_name('Love Potion'), 'floorPrice': 2.7, 'probability': 0.15 },
            { 'name': 'Top Hat', 'imageFilename': generate_image_filename_from_name('Top Hat'), 'floorPrice': 3, 'probability': 0.15 },
            { 'name': 'Voodoo Doll', 'imageFilename': generate_image_filename_from_name('Voodoo Doll'), 'floorPrice': 4.2, 'probability': 0.10 },
            { 'name': 'Eternal Rose', 'imageFilename': generate_image_filename_from_name('Eternal Rose'), 'floorPrice': 5.5, 'probability': 0.10 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 6.3, 'probability': 0.05 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 5.7, 'probability': 0.05 },
        ]
    },
    { 
        'id': 'perfumebottle', 'name': 'Perfume Chest', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'priceTON': 10,
        'prizes': [
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 21, 'probability': 0.10 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.15 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.20 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 9.6, 'probability': 0.15 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': 10, 'probability': 0.10 },
            { 'name': 'Kissed Frog', 'imageFilename': generate_image_filename_from_name('Kissed Frog'), 'floorPrice': 9, 'probability': 0.10 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': 12, 'probability': 0.05 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 6.3, 'probability': 0.10 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 5.7, 'probability': 0.05 },
        ]
    },
    { 
        'id': 'vintagecigar', 'name': 'Vintage Cigar Safe', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'priceTON': 20,
        'prizes': [
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': 13, 'probability': 0.10 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 21, 'probability': 0.15 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.20 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.15 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': 10, 'probability': 0.15 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 9.6, 'probability': 0.10 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': 18, 'probability': 0.08 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': 17, 'probability': 0.07 },
        ]
    },
    { 
        'id': 'astralshard', 'name': 'Astral Shard Relic', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'priceTON': 50,
        'prizes': [
            { 'name': 'Astral Shard', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'floorPrice': 60, 'probability': 0.10 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': 13, 'probability': 0.15 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 21, 'probability': 0.15 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.10 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.10 },
            { 'name': 'Precious Peach', 'imageFilename': generate_image_filename_from_name('Precious Peach'), 'floorPrice': 60, 'probability': 0.10 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': 18, 'probability': 0.15 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': 17, 'probability': 0.10 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': 12, 'probability': 0.05 },
        ]
    },
    { 
        'id': 'plushpepe', 'name': 'Plush Pepe Hoard', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'priceTON': 100,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 560, 'probability': 0.10 },
            { 'name': 'Durov\'s Cap', 'imageFilename': generate_image_filename_from_name('Durov\'s Cap'), 'floorPrice': 150, 'probability': 0.40 },
            { 'name': 'Astral Shard', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'floorPrice': 60, 'probability': 0.50 },
        ]
    },
    { 
        'id': 'happypepe', 'name': 'Happy Pepe Treasure', 'imageFilename': generate_image_filename_from_name('Happy Pepe Kissed Frog'), 'priceTON': 600,
        'prizes': [
            { 'name': 'Kissed Frog Happy Pepe', 'imageFilename': generate_image_filename_from_name('Happy Pepe Kissed Frog'), 'floorPrice': 660, 'probability': 0.3636 },
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 560, 'probability': 0.6364 },
        ]
    },
    { 
        'id': 'amber', 'name': 'Amber Nebula Case', 'isBackgroundCase': True, 'bgImageFilename': 'Amber.png', 'overlayPrizeName': 'Swiss Watch', 'priceTON': 3, 
        'prizes': [
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.01 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.03 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 6.3, 'probability': 0.05 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 5.7, 'probability': 0.10 },
            { 'name': 'Eternal Rose', 'imageFilename': generate_image_filename_from_name('Eternal Rose'), 'floorPrice': 5.5, 'probability': 0.15 },
            { 'name': 'Voodoo Doll', 'imageFilename': generate_image_filename_from_name('Voodoo Doll'), 'floorPrice': 4.2, 'probability': 0.15 },
            { 'name': 'Top Hat', 'imageFilename': generate_image_filename_from_name('Top Hat'), 'floorPrice': 3, 'probability': 0.15 },
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': 2, 'probability': 0.08 },
            { 'name': 'Love Potion', 'imageFilename': generate_image_filename_from_name('Love Potion'), 'floorPrice': 2.7, 'probability': 0.08 },
            { 'name': 'Sakura Flower', 'imageFilename': generate_image_filename_from_name('Sakura Flower'), 'floorPrice': 2, 'probability': 0.08 },
            { 'name': 'Jelly Bunny', 'imageFilename': generate_image_filename_from_name('Jelly Bunny'), 'floorPrice': 1.8, 'probability': 0.06 },
            { 'name': 'Skull Flower', 'imageFilename': generate_image_filename_from_name('Skull Flower'), 'floorPrice': 1.7, 'probability': 0.06 },
        ]
    },
    { 
        'id': 'midnightblue', 'name': 'Midnight Blue Comet', 'isBackgroundCase': True, 'bgImageFilename': 'Midnight_Blue.png', 'overlayPrizeName': 'Precious Peach', 'priceTON': 3, 
        'prizes': [ 
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 9.6, 'probability': 0.01 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.03 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.06 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 6.3, 'probability': 0.10 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 5.7, 'probability': 0.15 },
            { 'name': 'Eternal Rose', 'imageFilename': generate_image_filename_from_name('Eternal Rose'), 'floorPrice': 5.5, 'probability': 0.15 },
            { 'name': 'Top Hat', 'imageFilename': generate_image_filename_from_name('Top Hat'), 'floorPrice': 3, 'probability': 0.15 },
            { 'name': 'Love Potion', 'imageFilename': generate_image_filename_from_name('Love Potion'), 'floorPrice': 2.7, 'probability': 0.08 },
            { 'name': 'Tama Gadget', 'imageFilename': generate_image_filename_from_name('Tama Gadget'), 'floorPrice': 2, 'probability': 0.08 },
            { 'name': 'Snow Globe', 'imageFilename': generate_image_filename_from_name('Snow Globe'), 'floorPrice': 2, 'probability': 0.08 },
            { 'name': 'Sleigh Bell', 'imageFilename': generate_image_filename_from_name('Sleigh Bell'), 'floorPrice': 2, 'probability': 0.06 },
            { 'name': 'Candy Cane', 'imageFilename': generate_image_filename_from_name('Candy Cane'), 'floorPrice': 0.9, 'probability': 0.05 },
        ]
    },
    { 
        'id': 'onyxblack', 'name': 'Onyx Black Hole', 'isBackgroundCase': True, 'bgImageFilename': 'Onyx_Black.png', 'overlayPrizeName': 'Perfume Bottle', 'priceTON': 5, 
        'prizes': [ 
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': 10, 'probability': 0.01 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 9.6, 'probability': 0.03 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.05 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.10 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 6.3, 'probability': 0.15 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 5.7, 'probability': 0.15 },
            { 'name': 'Eternal Rose', 'imageFilename': generate_image_filename_from_name('Eternal Rose'), 'floorPrice': 5.5, 'probability': 0.15 },
            { 'name': 'Voodoo Doll', 'imageFilename': generate_image_filename_from_name('Voodoo Doll'), 'floorPrice': 4.2, 'probability': 0.10 },
            { 'name': 'Top Hat', 'imageFilename': generate_image_filename_from_name('Top Hat'), 'floorPrice': 3, 'probability': 0.08 },
            { 'name': 'Toy Bear', 'imageFilename': generate_image_filename_from_name('Toy Bear'), 'floorPrice': 5.2, 'probability': 0.08 },
            { 'name': 'Love Potion', 'imageFilename': generate_image_filename_from_name('Love Potion'), 'floorPrice': 2.7, 'probability': 0.05 },
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': 2, 'probability': 0.05 },
        ]
    },
    { 
        'id': 'black', 'name': 'BLACK Singularity', 'isBackgroundCase': True, 'bgImageFilename': 'Black.png', 'overlayPrizeName': 'Neko Helmet', 'priceTON': 15, 
        'prizes': [ 
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 21, 'probability': 0.01 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': 18, 'probability': 0.03 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': 17, 'probability': 0.05 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': 13, 'probability': 0.10 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': 12, 'probability': 0.15 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': 10, 'probability': 0.15 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 9.6, 'probability': 0.15 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 9, 'probability': 0.10 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 7.5, 'probability': 0.10 },
            { 'name': 'Kissed Frog', 'imageFilename': generate_image_filename_from_name('Kissed Frog'), 'floorPrice': 9, 'probability': 0.08 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 6.3, 'probability': 0.05 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 5.7, 'probability': 0.03 },
        ]
    },
]
# 🔴🔴🔴 КОНЕЦ СЕКЦИИ ДЛЯ ВСТАВКИ cases_data_backend 🔴🔴🔴

if not cases_data_backend:
    logger.critical("Массив cases_data_backend ПУСТ! Приложение не сможет корректно функционировать. Заполни его!")
    # Можно даже завершить приложение, если это критично для старта
    # exit("CRITICAL: cases_data_backend is empty. Halting application.")

def temp_alter_column_types():
    db_session = SessionLocal()
    try:
        logger.info("Попытка изменить типы столбцов на BIGINT...")
        
        # Сначала inventory_items.user_id, так как он ссылается на users.id
        # Если есть ограничение внешнего ключа, его может потребоваться временно удалить
        # Но попробуем сначала так, PostgreSQL может быть достаточно умен
        db_session.execute(text("ALTER TABLE inventory_items ALTER COLUMN user_id TYPE BIGINT;"))
        logger.info("Тип inventory_items.user_id изменен на BIGINT (если не было ошибки).")

        db_session.execute(text("ALTER TABLE users ALTER COLUMN referred_by_id TYPE BIGINT;"))
        logger.info("Тип users.referred_by_id изменен на BIGINT (если не было ошибки).")
        
        # users.id меняем последним, если другие таблицы на него ссылались старым типом
        db_session.execute(text("ALTER TABLE users ALTER COLUMN id TYPE BIGINT;"))
        logger.info("Тип users.id изменен на BIGINT (если не было ошибки).")
        
        db_session.commit()
        logger.info("Изменения типов столбцов успешно применены.")
    except Exception as e:
        db_session.rollback()
        logger.error(f"Ошибка при изменении типов столбцов: {e}")
        logger.error("ВАЖНО: Если это ошибка внешнего ключа, тебе может потребоваться сначала удалить ограничение, изменить типы, а затем снова добавить ограничение.")
    finally:
        db_session.close()

def populate_initial_nfts_from_cases():
    if not cases_data_backend:
        logger.error("Не могу заполнить NFT, так как cases_data_backend пуст.")
        return
    db = SessionLocal()
    try:
        existing_nft_names_query = db.query(NFT.name).all()
        existing_nft_names = {name_tuple[0] for name_tuple in existing_nft_names_query}
        
        nfts_to_add = []
        for case_config in cases_data_backend: 
            for prize in case_config.get('prizes', []):
                if prize['name'] not in existing_nft_names:
                    image_fn = prize.get('imageFilename', generate_image_filename_from_name(prize['name']))
                    nfts_to_add.append(NFT(
                        name=prize['name'], image_filename=image_fn, floor_price=prize['floorPrice']
                    ))
                    existing_nft_names.add(prize['name']) 

        if nfts_to_add:
            db.add_all(nfts_to_add)
            db.commit()
            logger.info(f"Добавлено {len(nfts_to_add)} новых NFT в базу.")
        else:
            logger.info("Новых NFT для добавления не найдено, или таблица уже заполнена.")
    except IntegrityError:
        db.rollback()
        logger.warning("Ошибка целостности при добавлении NFT (возможно, дубликаты уже существуют). Пропускаем.")
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при заполнении таблицы NFT: {type(e).__name__} - {e}")
    finally:
        db.close()

populate_initial_nfts_from_cases()

# --- Flask Приложение ---
app = Flask(__name__)

# Также, если ты тестируешь локально с Live Server, добавь его origin.
allowed_origins = [
    "https://vasiliy-katsyka.github.io", 
    # Если тестируешь локально с разными портами, добавь их:
    # "http://127.0.0.1:5500", # Пример для Live Server
    # "http://localhost:5500"  # Пример для Live Server
]

# Применяем CORS ко всем маршрутам, начинающимся с /api/
CORS(app, resources={r"/api/*": {"origins": allowed_origins}})

# --- Telegram Бот ---
if not BOT_TOKEN: 
    logger.error("Токен бота (BOT_TOKEN) не найден!")
    if __name__ == '__main__': exit("BOT_TOKEN is not set. Exiting.")
    else: raise RuntimeError("BOT_TOKEN is not set. Cannot initialize bot.")
bot = telebot.TeleBot(BOT_TOKEN)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Валидация Telegram WebApp InitData ---
def validate_init_data(init_data_str: str, bot_token: str) -> dict | None:
    try:
        parsed_data = dict(parse_qs(init_data_str))
        if 'hash' not in parsed_data or 'user' not in parsed_data or 'auth_date' not in parsed_data:
            logger.warning("initData missing required fields (hash, user, or auth_date).")
            return None

        hash_received = parsed_data.pop('hash')[0]
        auth_date_ts = int(parsed_data['auth_date'][0])
        current_ts = int(dt.now(timezone.utc).timestamp())

        if (current_ts - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS:
            logger.warning(f"initData is outdated. auth_date: {auth_date_ts}, current_ts: {current_ts}, diff: {current_ts - auth_date_ts}s. Max age: {AUTH_DATE_MAX_AGE_SECONDS}s")
            return None 

        data_check_list = []
        for key in sorted(parsed_data.keys()):
            data_check_list.append(f"{key}={parsed_data[key][0]}")
        data_check_string = "\n".join(data_check_list)

        secret_key_intermediate = bot_token.encode()
        key_for_secret = "WebAppData".encode()
        secret_key = hmac.new(key_for_secret, secret_key_intermediate, hashlib.sha256).digest()
        
        calculated_hash_bytes = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
        calculated_hash_hex = calculated_hash_bytes.hexdigest()

        if calculated_hash_hex == hash_received:
            user_data_json_str = unquote(parsed_data['user'][0])
            user_info_dict = json.loads(user_data_json_str) 
            return {
                "id": int(user_info_dict.get("id")), 
                "first_name": user_info_dict.get("first_name"),
                "last_name": user_info_dict.get("last_name"),
                "username": user_info_dict.get("username"),
                "language_code": user_info_dict.get("language_code"),
                "is_premium": user_info_dict.get("is_premium", False),
                "photo_url": user_info_dict.get("photo_url")
            }
        else:
            logger.warning(f"Hash mismatch! Received: {hash_received}, Calculated: {calculated_hash_hex}")
            logger.debug(f"DataCheckString for mismatch: '{data_check_string}'")
            return None
    except Exception as e:
        logger.error(f"Exception during initData validation: {type(e).__name__} - {e}")
        return None

# --- API Эндпоинты ---
@app.route('/') 
def index_route(): 
    return "Flask App (Full Backend - Cases Omitted - BigInt Fix) is running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Authentication failed"}), 401
    
    user_id = auth_user_data["id"]
    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: 
        logger.warning(f"User {user_id} not found via API, should be created by /start. Creating now.")
        user = User(
            id=user_id, username=auth_user_data.get("username"),
            first_name=auth_user_data.get("first_name"), last_name=auth_user_data.get("last_name"),
            ton_balance=0.0, star_balance=0,
            referral_code=f"ref_{user_id}_{random.randint(1000,9999)}"
        )
        db.add(user)
        try:
            db.commit()
            db.refresh(user)
        except Exception as e_commit:
            db.rollback()
            logger.error(f"Error creating user {user_id} via API: {e_commit}")
            return jsonify({"error": "Failed to initialize user data"}), 500


    inventory_data = []
    for item in user.inventory:
        inventory_data.append({
            "id": item.id, "name": item.nft.name, "imageFilename": item.nft.image_filename,
            "floorPrice": item.nft.floor_price, "currentValue": item.current_value,
            "upgradeMultiplier": item.upgrade_multiplier,
            "obtained_at": item.obtained_at.isoformat() if item.obtained_at else None
        })
    
    return jsonify({
        "id": user.id, "username": user.username, "first_name": user.first_name,
        "last_name": user.last_name, "tonBalance": user.ton_balance,
        "starBalance": user.star_balance, "inventory": inventory_data,
        "referralCode": user.referral_code,
        "referralEarningsPending": user.referral_earnings_pending,
        "total_won_ton": user.total_won_ton
    })

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    case_id = data.get('case_id')
    if not case_id: return jsonify({"error": "case_id is required"}), 400

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404

    if not cases_data_backend: return jsonify({"error": "Case data not loaded on backend"}), 500
    target_case = next((c for c in cases_data_backend if c['id'] == case_id), None)
    if not target_case: return jsonify({"error": "Case not found"}), 404

    case_cost_ton = target_case.get('priceTON', 0)
    if user.ton_balance < case_cost_ton:
        return jsonify({"error": f"Not enough TON. Need {case_cost_ton}, have {user.ton_balance:.2f}"}), 400
    
    prizes = target_case.get('prizes', [])
    if not prizes: return jsonify({"error": "No prizes in this case"}), 500

    total_probability = sum(p.get('probability', 0) for p in prizes)
    winner_data = None
    if total_probability == 0 and prizes: 
        winner_data = random.choice(prizes)
    elif total_probability > 0:
        normalized_prizes = []
        if abs(total_probability - 1.0) > 0.0001: 
            logger.warning(f"Probabilities for case {case_id} do not sum to 1 (sum={total_probability}). Normalizing.")
            for p_info in prizes:
                normalized_prizes.append({**p_info, 'probability': p_info.get('probability',0) / total_probability})
        else:
            normalized_prizes = prizes
        
        rand_val = random.random() 
        current_prob_sum = 0
        for prize_info in normalized_prizes:
            current_prob_sum += prize_info.get('probability', 0)
            if rand_val <= current_prob_sum:
                winner_data = prize_info
                break
        if not winner_data: winner_data = random.choice(normalized_prizes) # Fallback
    else: 
        return jsonify({"error": "Case prize configuration error"}), 500
    
    if not winner_data: return jsonify({"error": "Could not determine prize"}), 500

    user.ton_balance -= case_cost_ton
    user.total_won_ton += winner_data['floorPrice'] 

    db_nft = db.query(NFT).filter(NFT.name == winner_data['name']).first()
    if not db_nft:
        logger.error(f"NFT '{winner_data['name']}' NOT FOUND in DB. This indicates an issue with populate_initial_nfts_from_cases or missing NFT in cases_data_backend.")
        image_fn_winner = winner_data.get('imageFilename', generate_image_filename_from_name(winner_data['name']))
        db_nft = NFT(name=winner_data['name'], image_filename=image_fn_winner, floor_price=winner_data['floorPrice'])
        db.add(db_nft)
        try:
            db.commit(); db.refresh(db_nft)
            logger.info(f"NFT '{winner_data['name']}' created on-the-fly.")
        except Exception as e_create:
            db.rollback(); logger.error(f"Failed to create NFT '{winner_data['name']}' on-the-fly: {e_create}")
            user.ton_balance += case_cost_ton; user.total_won_ton -= winner_data['floorPrice']
            db.commit()
            return jsonify({"error": "Internal prize data error, NFT creation failed"}), 500

    new_item = InventoryItem(
        user_id=user.id, nft_id=db_nft.id,
        current_value=db_nft.floor_price, upgrade_multiplier=1.0 
    )
    db.add(new_item); db.commit(); db.refresh(new_item) 
    
    return jsonify({
        "status": "success",
        "won_prize": {
            "id": new_item.id, "name": db_nft.name, "imageFilename": db_nft.image_filename,
            "floorPrice": db_nft.floor_price, "currentValue": new_item.current_value
        },
        "new_balance_ton": user.ton_balance,
    })

@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    inventory_item_id = data.get('inventory_item_id')
    multiplier_str = data.get('multiplier_str') 

    if not all([inventory_item_id, multiplier_str]):
        return jsonify({"error": "inventory_item_id and multiplier_str are required"}), 400
    
    try:
        multiplier = float(multiplier_str) 
        inventory_item_id = int(inventory_item_id)
    except ValueError:
        return jsonify({"error": "Invalid data format for multiplier or item_id"}), 400

    upgrade_chances = {1.5: 50, 2.0: 35, 3.0: 25, 5.0: 15, 10.0: 8, 20.0: 3} 
    if multiplier not in upgrade_chances:
        return jsonify({"error": f"Invalid multiplier: {multiplier}. Valid are {list(upgrade_chances.keys())}" }), 400
    
    success_chance = upgrade_chances[multiplier]

    db = next(get_db())
    item_to_upgrade = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()

    if not item_to_upgrade:
        return jsonify({"error": "Item not found in your inventory"}), 404

    if random.uniform(0, 100) < success_chance:
        new_value = round(item_to_upgrade.current_value * multiplier, 2)
        diff_value = new_value - item_to_upgrade.current_value
        item_to_upgrade.current_value = new_value
        item_to_upgrade.upgrade_multiplier *= multiplier
        
        user = db.query(User).filter(User.id == user_id).first() 
        if user: user.total_won_ton += diff_value 
        
        db.commit()
        return jsonify({
            "status": "success", 
            "message": f"Upgrade successful! New value: {item_to_upgrade.current_value:.2f} TON",
            "item": {"id": item_to_upgrade.id, "currentValue": item_to_upgrade.current_value, "name": item_to_upgrade.nft.name, "upgradeMultiplier": item_to_upgrade.upgrade_multiplier }
        })
    else:
        item_name_lost = item_to_upgrade.nft.name
        lost_value = item_to_upgrade.current_value 
        
        user = db.query(User).filter(User.id == user_id).first() 
        if user: user.total_won_ton -= lost_value
        
        db.delete(item_to_upgrade)
        db.commit()
        logger.info(f"Item {item_name_lost} (ID: {inventory_item_id}, Value: {lost_value}) from user {user_id} lost in upgrade.")
        return jsonify({
            "status": "failed", 
            "message": f"Upgrade failed! You lost {item_name_lost}."
        })

@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    inventory_item_id = data.get('inventory_item_id')

    if not inventory_item_id: return jsonify({"error": "inventory_item_id is required"}), 400
    try: inventory_item_id = int(inventory_item_id)
    except ValueError: return jsonify({"error": "Invalid inventory_item_id"}), 400
    
    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    item_to_convert = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()

    if not user: return jsonify({"error": "User not found"}), 404
    if not item_to_convert: return jsonify({"error": "Item not found in inventory"}), 404

    conversion_value = item_to_convert.current_value 
    user.ton_balance += conversion_value
    db.delete(item_to_convert)
    db.commit()

    return jsonify({
        "status": "success",
        "message": f"{item_to_convert.nft.name} converted to {conversion_value:.2f} TON.",
        "new_balance_ton": user.ton_balance
    })

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404

    if not user.inventory:
        return jsonify({"status": "no_items", "message": "Inventory is empty."})

    total_sell_value = sum(item.current_value for item in user.inventory)
    user.ton_balance += total_sell_value
    
    for item in user.inventory: db.delete(item)
    db.commit()

    return jsonify({
        "status": "success",
        "message": f"All items sold for {total_sell_value:.2f} TON.",
        "new_balance_ton": user.ton_balance
    })

@app.route('/api/deposit_ton', methods=['POST'])
def deposit_ton_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    amount_str = data.get('amount') 
    if amount_str is None: return jsonify({"error": "amount is required"}), 400
    try: amount = float(amount_str)
    except ValueError: return jsonify({"error": "Invalid amount format"}), 400
    if amount <= 0: return jsonify({"error": "Amount must be positive"}), 400

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404

    user.ton_balance += amount
    if user.referred_by_id:
        referrer = db.query(User).filter(User.id == user.referred_by_id).first()
        if referrer:
            referral_bonus = round(amount * 0.10, 2) 
            referrer.referral_earnings_pending += referral_bonus
            logger.info(f"Начислено {referral_bonus} TON рефереру {referrer.id} от пополнения {user.id}")
    db.commit()
    return jsonify({
        "status": "success",
        "message": f"{amount:.2f} TON deposited successfully (Test).",
        "new_balance_ton": user.ton_balance
    })

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    leaders_query = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
    leaderboard_data = []
    for rank, user_leader in enumerate(leaders_query, 1):
        leaderboard_data.append({
            "rank": rank,
            "name": user_leader.first_name or user_leader.username or f"User_{user_leader.id}",
            "avatarChar": (user_leader.first_name or user_leader.username or "U")[0].upper(),
            "income": user_leader.total_won_ton,
            "user_id": user_leader.id 
        })
    return jsonify(leaderboard_data)

@app.route('/api/withdraw_referral_earnings', methods=['POST'])
def withdraw_referral_earnings_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404

    if user.referral_earnings_pending > 0:
        amount_withdrawn = user.referral_earnings_pending
        user.ton_balance += amount_withdrawn
        user.referral_earnings_pending = 0
        db.commit()
        return jsonify({
            "status": "success",
            "message": f"{amount_withdrawn:.2f} TON referral earnings withdrawn.",
            "new_balance_ton": user.ton_balance,
            "new_referral_earnings_pending": user.referral_earnings_pending
        })
    else:
        return jsonify({"status": "no_earnings", "message": "No referral earnings to withdraw."})

# --- Команды бота ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"Получена команда /start от chat_id: {message.chat.id} ({message.from_user.username or 'N/A'})")
    db = next(get_db())
    user = db.query(User).filter(User.id == message.chat.id).first()
    created_now = False
    if not user:
        created_now = True
        user = User(
            id=message.chat.id, 
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
            ton_balance=0.0, 
            star_balance=0,
            referral_code=f"ref_{message.chat.id}_{random.randint(1000,9999)}"
        )
        db.add(user) 
    
    try:
        start_param = message.text.split(' ')
        if len(start_param) > 1 and start_param[1].startswith('ref_'):
            referrer_code_param = start_param[1]
            if created_now and not user.referred_by_id: 
                referrer = db.query(User).filter(User.referral_code == referrer_code_param).first()
                if referrer and referrer.id != user.id :
                    user.referred_by_id = referrer.id
                    logger.info(f"Пользователь {user.id} пришел по реф. коду {referrer_code_param} от {referrer.id}")
    except Exception as e:
        logger.error(f"Ошибка обработки реферального параметра для {user.id}: {e}")

    changed_in_db = False
    if user.username != message.from_user.username: user.username = message.from_user.username; changed_in_db=True
    if user.first_name != message.from_user.first_name: user.first_name = message.from_user.first_name; changed_in_db=True
    if user.last_name != message.from_user.last_name: user.last_name = message.from_user.last_name; changed_in_db=True
    
    if created_now or changed_in_db:
        try:
            db.commit()
            if created_now: logger.info(f"Новый пользователь {message.chat.id} ({message.from_user.username or 'N/A'}) добавлен/обновлен в БД.")
            elif changed_in_db: logger.info(f"Данные пользователя {message.chat.id} обновлены.")
        except Exception as e_commit:
            db.rollback()
            logger.error(f"Ошибка сохранения пользователя {message.chat.id}: {e_commit}")

    markup = types.InlineKeyboardMarkup()
    if not MINI_APP_URL:
        logger.error("MINI_APP_URL не установлен!")
        bot.send_message(message.chat.id, "Ошибка конфигурации: Mini App URL не найден.")
        return
    
    try:
        web_app_info = types.WebAppInfo(url=MINI_APP_URL) 
        app_button = types.InlineKeyboardButton(text="🎮 Открыть Игру-Рулетку", web_app=web_app_info)
        markup.add(app_button)
        bot.send_message(
            message.chat.id,
            "Добро пожаловать в TON Gift Universe! 🎁\n\n"
            "Нажмите кнопку ниже, чтобы открыть рулетку и испытать свою удачу!",
            reply_markup=markup
        )
    except Exception as e: 
        logger.error(f"Ошибка при отправке /start ({message.chat.id}): {type(e).__name__} - {e}")
        try:
             bot.send_message(message.chat.id, "Произошла ошибка при открытии игры. Попробуйте позже.")
        except Exception as e2:
            logger.error(f"Не удалось отправить сообщение об ошибке пользователю {message.chat.id}: {e2}")

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    logger.info(f"Получено сообщение от {message.chat.id}: {message.text}")
    bot.reply_to(message, "Нажмите /start, чтобы открыть игру.")

# --- Polling ---
bot_polling_started = False
bot_polling_thread = None
def run_bot_polling():
    global bot_polling_started
    if bot_polling_started: logger.info("Polling уже запущен."); return
    bot_polling_started = True
    logger.info("Запуск бота в режиме polling...")    
    max_retries_remove_webhook = 3
    for i in range(max_retries_remove_webhook):
        try:
            bot.remove_webhook()
            logger.info("Вебхук успешно удален (если был).")
            break
        except Exception as e:
            logger.warning(f"Попытка {i+1}/{max_retries_remove_webhook} удалить вебхук не удалась: {e}")
            if i < max_retries_remove_webhook - 1: time.sleep(2)
            else: logger.error("Не удалось удалить вебхук после нескольких попыток.")    
    while True: 
        try:
            logger.info("Старт infinity_polling...")
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"Ошибка API Telegram в polling: {e}. Код: {e.error_code}")
            if e.error_code == 401: logger.error("Неверный токен бота. Polling остановлен."); bot_polling_started=False; break 
            elif e.error_code == 409: logger.error("Конфликт вебхука. Polling остановлен."); bot_polling_started=False; break
            else: logger.error(f"Другая ошибка API Telegram, перезапуск polling через 30 секунд..."); time.sleep(30)
        except ConnectionError as e: logger.error(f"Ошибка соединения: {e}. Перезапуск через 60 секунд..."); time.sleep(60)
        except Exception as e: logger.error(f"Критическая ошибка в polling: {e}. Перезапуск через 60 секунд..."); time.sleep(60)
        else: logger.warning("infinity_polling завершился. Перезапуск через 15 секунд..."); time.sleep(15)
        if not bot_polling_started: break


if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
    bot_polling_thread = threading.Thread(target=run_bot_polling)
    bot_polling_thread.daemon = True
    bot_polling_thread.start()
    logger.info("Поток для polling бота запущен.")

if __name__ == '__main__':
    logger.info("Запуск Flask development server...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=False)
