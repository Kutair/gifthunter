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
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import text 

# Pytoniq imports (Ensure it's installed: pip install pytoniq)
from pytoniq import LiteBalancer 
import asyncio


load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_URL = os.environ.get("MINI_APP_URL", "https://default_mini_app_url.io") 
DATABASE_URL = os.environ.get("DATABASE_URL") 
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24 # 24 hours for initData validity

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
    star_balance = Column(Integer, default=0, nullable=False) # Keep if you plan to use stars
    referral_code = Column(String, unique=True, index=True, nullable=True)
    referred_by_id = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    referral_earnings_pending = Column(Float, default=0.0, nullable=False)
    total_won_ton = Column(Float, default=0.0, nullable=False) 
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    
    inventory = relationship("InventoryItem", back_populates="owner", cascade="all, delete-orphan")
    pending_deposits = relationship("PendingDeposit", back_populates="owner")
    # For promocode tracking (optional, if one user can activate a code only once)
    # activated_promocodes = relationship("UserPromoCodeActivation", back_populates="user")
    
    # Relationship for referrals made by this user
    referrals_made = relationship("User", backref=relationship("referrer", remote_side=[id]), foreign_keys=[referred_by_id])


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

class PendingDeposit(Base):
    __tablename__ = "pending_deposits"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    original_amount_ton = Column(Float, nullable=False)
    unique_identifier_nano_ton = Column(BigInteger, nullable=False) # The small random part
    final_amount_nano_ton = Column(BigInteger, nullable=False, index=True) # original_nano + unique_nano
    expected_comment = Column(String, nullable=False, default="cpd7r07ud3s") # Default, can be overridden
    status = Column(String, default="pending", index=True) # pending, completed, expired, failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)
    
    owner = relationship("User", back_populates="pending_deposits")

# Point 2: Promocode Model
class PromoCode(Base):
    __tablename__ = "promo_codes"
    id = Column(Integer, primary_key=True, index=True)
    code_text = Column(String, unique=True, index=True, nullable=False)
    activations_left = Column(Integer, nullable=False, default=0)
    ton_amount = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    
    # To track who activated which code (optional for more advanced tracking)
    # activations = relationship("UserPromoCodeActivation", back_populates="promocode")

# Optional: For tracking individual user activations of specific promocodes
# class UserPromoCodeActivation(Base):
#     __tablename__ = "user_promo_code_activations"
#     id = Column(Integer, primary_key=True, index=True)
#     user_id = Column(BigInteger, ForeignKey("users.id"), nullable=False)
#     promo_code_id = Column(Integer, ForeignKey("promo_codes.id"), nullable=False)
#     activated_at = Column(DateTime(timezone=True), server_default=func.now())
#     __table_args__ = (UniqueConstraint('user_id', 'promo_code_id', name='uq_user_promocode'),)
#     user = relationship("User", back_populates="activated_promocodes")
#     promocode = relationship("PromoCode", back_populates="activations")


Base.metadata.create_all(bind=engine)


# --- Функция генерации имени файла (Placeholder for your actual function) ---
# 🔴🔴🔴 PASTE YOUR `generate_image_filename_from_name` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
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


# --- Данные кейсов (Placeholder for your actual data) ---
# 🔴🔴🔴 PASTE YOUR `cases_data_backend` ARRAY HERE (BUT UPDATE 'black' CASE's bgImageFilename) 🔴🔴🔴
# Example of how to update 'black' case:
# { 
#    'id': 'black', 'name': 'BLACK Singularity', 
#    'isBackgroundCase': True, 
#    'bgImageFilename': 'image-1.png', # POINT 5: Updated filename (actual URL is frontend responsibility)
#    'overlayPrizeName': 'Neko Helmet', 
#    'priceTON': 30, 
#    'prizes': [ /* ... your prizes ... */ ]
# },
cases_data_backend = [
    { 
        'id': 'lolpop', 'name': 'Lol Pop Stash', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'priceTON': 1.5,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.001 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 15, 'probability': 0.005 },
            { 'name': 'Party Sparkler', 'imageFilename': generate_image_filename_from_name('Party Sparkler'), 'floorPrice': 2, 'probability': 0.07 },
            { 'name': 'Homemade Cake', 'imageFilename': generate_image_filename_from_name('Homemade Cake'), 'floorPrice': 2, 'probability': 0.07 },
            { 'name': 'Cookie Heart', 'imageFilename': generate_image_filename_from_name('Cookie Heart'), 'floorPrice': 1.8, 'probability': 0.07 },
            { 'name': 'Jack-in-the-box', 'imageFilename': generate_image_filename_from_name('Jack-in-the-box'), 'floorPrice': 2, 'probability': 0.06 },
            { 'name': 'Skull Flower', 'imageFilename': generate_image_filename_from_name('Skull Flower'), 'floorPrice': 3.4, 'probability': 0.023 },
            { 'name': 'Lol Pop', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'floorPrice': 1.4, 'probability': 0.25 },
            { 'name': 'Hynpo Lollipop', 'imageFilename': generate_image_filename_from_name('Hynpo Lollipop'), 'floorPrice': 1.4, 'probability': 0.25 },
            { 'name': 'Desk Calendar', 'imageFilename': generate_image_filename_from_name('Desk Calendar'), 'floorPrice': 1.4, 'probability': 0.10 },
            { 'name': 'B-Day Candle', 'imageFilename': generate_image_filename_from_name('B-Day Candle'), 'floorPrice': 1.4, 'probability': 0.10 },
        ]
    },
    { 
        'id': 'recordplayer', 'name': 'Record Player Vault', 'imageFilename': generate_image_filename_from_name('Record Player'), 'priceTON': 6,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.0012 },
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': 4, 'probability': 0.40 },
            { 'name': 'Lol Pop', 'imageFilename': generate_image_filename_from_name('Lol Pop'), 'floorPrice': 1.4, 'probability': 0.10 },
            { 'name': 'Hynpo Lollipop', 'imageFilename': generate_image_filename_from_name('Hynpo Lollipop'), 'floorPrice': 1.4, 'probability': 0.10 },
            { 'name': 'Party Sparkler', 'imageFilename': generate_image_filename_from_name('Party Sparkler'), 'floorPrice': 2, 'probability': 0.10 },
            { 'name': 'Skull Flower', 'imageFilename': generate_image_filename_from_name('Skull Flower'), 'floorPrice': 3.4, 'probability': 0.10 },
            { 'name': 'Jelly Bunny', 'imageFilename': generate_image_filename_from_name('Jelly Bunny'), 'floorPrice': 3.6, 'probability': 0.0988 },
            { 'name': 'Tama Gadget', 'imageFilename': generate_image_filename_from_name('Tama Gadget'), 'floorPrice': 4, 'probability': 0.05 },
            { 'name': 'Snow Globe', 'imageFilename': generate_image_filename_from_name('Snow Globe'), 'floorPrice': 4, 'probability': 0.05 },
        ]
    },
    { 
        'id': 'swisswatch', 'name': 'Swiss Watch Box', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'priceTON': 10,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.0015 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 18, 'probability': 0.08 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 15, 'probability': 0.10 },
            { 'name': 'Eternal Rose', 'imageFilename': generate_image_filename_from_name('Eternal Rose'), 'floorPrice': 11, 'probability': 0.05 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 12.6, 'probability': 0.03 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 11.4, 'probability': 0.0395 },
            { 'name': 'Record Player', 'imageFilename': generate_image_filename_from_name('Record Player'), 'floorPrice': 4, 'probability': 0.20 },
            { 'name': 'Love Potion', 'imageFilename': generate_image_filename_from_name('Love Potion'), 'floorPrice': 5.4, 'probability': 0.20 },
            { 'name': 'Top Hat', 'imageFilename': generate_image_filename_from_name('Top Hat'), 'floorPrice': 6, 'probability': 0.15 },
            { 'name': 'Voodoo Doll', 'imageFilename': generate_image_filename_from_name('Voodoo Doll'), 'floorPrice': 8.4, 'probability': 0.149 },
        ]
    },
    { 
        'id': 'perfumebottle', 'name': 'Perfume Chest', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'priceTON': 20,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.0018 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 42, 'probability': 0.08 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': 20, 'probability': 0.12 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': 24, 'probability': 0.09946 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 18, 'probability': 0.15 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 15, 'probability': 0.15 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 19.2, 'probability': 0.15 },
            { 'name': 'Kissed Frog', 'imageFilename': generate_image_filename_from_name('Kissed Frog'), 'floorPrice': 18, 'probability': 0.10 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 12.6, 'probability': 0.07 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 11.4, 'probability': 0.07874 },
        ]
    },
    { 
        'id': 'vintagecigar', 'name': 'Vintage Cigar Safe', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'priceTON': 40,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.002 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 42, 'probability': 0.2994 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': 26, 'probability': 0.12 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 18, 'probability': 0.12 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 15, 'probability': 0.10 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': 20, 'probability': 0.10 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 19.2, 'probability': 0.08 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': 36, 'probability': 0.08 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': 34, 'probability': 0.05 },
            { 'name': 'Toy Bear', 'imageFilename': generate_image_filename_from_name('Toy Bear'), 'floorPrice': 15, 'probability': 0.0486 },
        ]
    },
    { 
        'id': 'astralshard', 'name': 'Astral Shard Relic', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'priceTON': 100,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.0025 },
            { 'name': 'Durov\'s Cap', 'imageFilename': generate_image_filename_from_name('Durov\'s Cap'), 'floorPrice': 200, 'probability': 0.09925 },
            { 'name': 'Astral Shard', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'floorPrice': 120, 'probability': 0.10 },
            { 'name': 'Precious Peach', 'imageFilename': generate_image_filename_from_name('Precious Peach'), 'floorPrice': 120, 'probability': 0.10 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': 26, 'probability': 0.12 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 42, 'probability': 0.12 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 18, 'probability': 0.10 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 15, 'probability': 0.08 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': 36, 'probability': 0.10 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': 34, 'probability': 0.08 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': 24, 'probability': 0.05 },
            { 'name': 'Toy Bear', 'imageFilename': generate_image_filename_from_name('Toy Bear'), 'floorPrice': 15, 'probability': 0.04825 },
        ]
    },
    { 
        'id': 'plushpepe', 'name': 'Plush Pepe Hoard', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'priceTON': 200,
        'prizes': [
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.15 },
            { 'name': 'Durov\'s Cap', 'imageFilename': generate_image_filename_from_name('Durov\'s Cap'), 'floorPrice': 200, 'probability': 0.25 },
            { 'name': 'Astral Shard', 'imageFilename': generate_image_filename_from_name('Astral Shard'), 'floorPrice': 120, 'probability': 0.60 },
        ]
    },
    { 
        'id': 'black', 'name': 'BLACK Singularity', 
        'isBackgroundCase': True, 'bgImageFilename': 'Black.png', 'overlayPrizeName': 'Neko Helmet', 
        'priceTON': 30, 
        'prizes': [ 
            { 'name': 'Plush Pepe', 'imageFilename': generate_image_filename_from_name('Plush Pepe'), 'floorPrice': 1000, 'probability': 0.0022 },
            { 'name': 'Durov\'s Cap', 'imageFilename': generate_image_filename_from_name('Durov\'s Cap'), 'floorPrice': 200, 'probability': 0.04934 },
            { 'name': 'Perfume Bottle', 'imageFilename': generate_image_filename_from_name('Perfume Bottle'), 'floorPrice': 42, 'probability': 0.10 },
            { 'name': 'Mini Oscar', 'imageFilename': generate_image_filename_from_name('Mini Oscar'), 'floorPrice': 36, 'probability': 0.08 },
            { 'name': 'Scared Cat', 'imageFilename': generate_image_filename_from_name('Scared Cat'), 'floorPrice': 34, 'probability': 0.07 },
            { 'name': 'Vintage Cigar', 'imageFilename': generate_image_filename_from_name('Vintage Cigar'), 'floorPrice': 26, 'probability': 0.08 },
            { 'name': 'Loot Bag', 'imageFilename': generate_image_filename_from_name('Loot Bag'), 'floorPrice': 24, 'probability': 0.08 },
            { 'name': 'Sharp Tongue', 'imageFilename': generate_image_filename_from_name('Sharp Tongue'), 'floorPrice': 20, 'probability': 0.08 },
            { 'name': 'Genie Lamp', 'imageFilename': generate_image_filename_from_name('Genie Lamp'), 'floorPrice': 19.2, 'probability': 0.08 },
            { 'name': 'Swiss Watch', 'imageFilename': generate_image_filename_from_name('Swiss Watch'), 'floorPrice': 18, 'probability': 0.07 },
            { 'name': 'Neko Helmet', 'imageFilename': generate_image_filename_from_name('Neko Helmet'), 'floorPrice': 15, 'probability': 0.07 },
            { 'name': 'Kissed Frog', 'imageFilename': generate_image_filename_from_name('Kissed Frog'), 'floorPrice': 18, 'probability': 0.07 },
            { 'name': 'Electric Skull', 'imageFilename': generate_image_filename_from_name('Electric Skull'), 'floorPrice': 12.6, 'probability': 0.05 },
            { 'name': 'Diamond Ring', 'imageFilename': generate_image_filename_from_name('Diamond Ring'), 'floorPrice': 11.4, 'probability': 0.05 },
            { 'name': 'Toy Bear', 'imageFilename': generate_image_filename_from_name('Toy Bear'), 'floorPrice': 15, 'probability': 0.06846 },
        ]
    },
]


if not cases_data_backend:
    logger.critical("Массив cases_data_backend ПУСТ! Приложение не сможет корректно функционировать. Заполни его!")

def populate_initial_data():
    if not cases_data_backend:
        logger.error("Не могу заполнить NFT, так как cases_data_backend пуст.")
        return
    db = SessionLocal()
    try:
        # Populate NFTs
        existing_nft_names_query = db.query(NFT.name).all()
        existing_nft_names = {name_tuple[0] for name_tuple in existing_nft_names_query}
        
        nfts_to_add = []
        for case_config in cases_data_backend: 
            for prize in case_config.get('prizes', []):
                if prize['name'] not in existing_nft_names:
                    # image_fn should be derived correctly if not directly provided in prize dict
                    image_fn = prize.get('imageFilename', generate_image_filename_from_name(prize['name']))
                    nfts_to_add.append(NFT(
                        name=prize['name'], image_filename=image_fn, floor_price=prize['floorPrice']
                    ))
                    existing_nft_names.add(prize['name']) # Add to set to avoid duplicates in this batch

        if nfts_to_add:
            db.add_all(nfts_to_add)
            db.commit()
            logger.info(f"Добавлено {len(nfts_to_add)} новых NFT в базу.")
        else:
            logger.info("Новых NFT для добавления не найдено, или таблица уже заполнена.")

        # Point 2: Seed "durov" promocode
        durov_code = db.query(PromoCode).filter(PromoCode.code_text == 'durov').first()
        if not durov_code:
            durov_code = PromoCode(code_text='durov', activations_left=10, ton_amount=5.0)
            db.add(durov_code)
            db.commit()
            logger.info("Promocode 'durov' (5 TON, 10 activations) seeded.")
        else:
            logger.info("Promocode 'durov' already exists.")

    except IntegrityError:
        db.rollback()
        logger.warning("Ошибка целостности при добавлении NFT/Promocode (возможно, дубликаты уже существуют). Пропускаем.")
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при заполнении таблицы NFT/Promocode: {type(e).__name__} - {e}")
    finally:
        db.close()

populate_initial_data()

# --- Constants for Deposit ---
DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR" # Your actual deposit address
DEPOSIT_COMMENT = "cpd7r07ud3s" # Your desired fixed comment
PENDING_DEPOSIT_EXPIRY_MINUTES = 30 

# --- Flask Приложение ---
app = Flask(__name__)
allowed_origins = [
    "https://vasiliy-katsyka.github.io", 
    # "http://127.0.0.1:5500", # For local dev if needed
    # "http://localhost:5500" # For local dev if needed
]
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
# 🔴🔴🔴 PASTE YOUR `validate_init_data` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
def validate_init_data(init_data_str: str, bot_token: str) -> dict | None:
    try:
        if not init_data_str:
            logger.warning("initData is empty or None.")
            return None
        
        # URL decode the entire string first
        # init_data_str_decoded = unquote(init_data_str) # Sometimes needed if the whole string is urlencoded
        # parsed_data = dict(parse_qs(init_data_str_decoded))
        parsed_data = dict(parse_qs(init_data_str))


        if 'hash' not in parsed_data or 'user' not in parsed_data or 'auth_date' not in parsed_data:
            logger.warning(f"initData missing required fields. Got: {list(parsed_data.keys())}")
            return None

        hash_received = parsed_data.pop('hash')[0]
        auth_date_ts = int(parsed_data['auth_date'][0])
        current_ts = int(dt.now(timezone.utc).timestamp())

        if (current_ts - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS:
            logger.warning(f"initData is outdated. auth_date: {auth_date_ts}, current_ts: {current_ts}, diff: {current_ts - auth_date_ts}s. Max age: {AUTH_DATE_MAX_AGE_SECONDS}s")
            return None 

        data_check_list = []
        # Keys must be sorted alphabetically before forming the data_check_string
        for key in sorted(parsed_data.keys()):
            # Values also need to be unquoted if they were part of the original query string
            # and parse_qs might leave them as lists.
            # value_str = unquote(parsed_data[key][0]) # Ensure value is unquoted if initData comes heavily encoded
            value_str = parsed_data[key][0] # parse_qs already decodes components
            data_check_list.append(f"{key}={value_str}")
        
        data_check_string = "\n".join(data_check_list)

        # HMAC-SHA256 signing
        secret_key_intermediate = bot_token.encode() # Bot token itself
        key_for_secret = "WebAppData".encode() # Constant string "WebAppData"
        secret_key = hmac.new(key_for_secret, secret_key_intermediate, hashlib.sha256).digest()
        
        calculated_hash_bytes = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256)
        calculated_hash_hex = calculated_hash_bytes.hexdigest()

        if calculated_hash_hex == hash_received:
            user_data_json_str = unquote(parsed_data['user'][0]) # User data is a JSON string, URL encoded
            user_info_dict = json.loads(user_data_json_str) 
            # Ensure 'id' is present and is an integer
            if 'id' not in user_info_dict or not isinstance(user_info_dict['id'], (int, float)): # float if from some JS sources
                logger.warning(f"User ID missing or not numeric in parsed user data: {user_info_dict}")
                return None
            
            return {
                "id": int(user_info_dict.get("id")), 
                "first_name": user_info_dict.get("first_name"),
                "last_name": user_info_dict.get("last_name"),
                "username": user_info_dict.get("username"),
                "language_code": user_info_dict.get("language_code"),
                "is_premium": user_info_dict.get("is_premium", False),
                "photo_url": user_info_dict.get("photo_url")
                # Add other fields from user object if needed
            }
        else:
            logger.warning(f"Hash mismatch! Received: {hash_received}, Calculated: {calculated_hash_hex}")
            logger.debug(f"DataCheckString for mismatch: '{data_check_string}'")
            return None
    except Exception as e:
        logger.error(f"Exception during initData validation: {type(e).__name__} - {e}", exc_info=True)
        return None


# --- API Эндпоинты ---
@app.route('/') 
def index_route(): 
    return "Pusik Gifts Flask App is running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Authentication failed"}), 401
    
    user_id = auth_user_data["id"]
    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: 
        logger.warning(f"User {user_id} not found via API, should have been created by /start. Creating now for robustness.")
        user = User(
            id=user_id, username=auth_user_data.get("username"),
            first_name=auth_user_data.get("first_name"), last_name=auth_user_data.get("last_name"),
            ton_balance=0.0, star_balance=0, # Default balances
            referral_code=f"ref_{user_id}_{random.randint(1000,9999)}" # Generate a referral code
        )
        db.add(user)
        try:
            db.commit()
            db.refresh(user)
        except IntegrityError: # Catch if referral code somehow clashes (rare) or other integrity issues
            db.rollback()
            user = db.query(User).filter(User.id == user_id).first() # Try to fetch again if created by race condition
            if not user:
                logger.error(f"Failed to create or fetch user {user_id} after IntegrityError.")
                return jsonify({"error": "Failed to initialize user data"}), 500
        except Exception as e_commit:
            db.rollback()
            logger.error(f"Error creating user {user_id} via API: {e_commit}")
            return jsonify({"error": "Failed to initialize user data"}), 500

    inventory_data = []
    for item in user.inventory: # Ensure inventory items are loaded with their NFT details
        inventory_data.append({
            "id": item.id, "name": item.nft.name, "imageFilename": item.nft.image_filename,
            "floorPrice": item.nft.floor_price, "currentValue": item.current_value,
            "upgradeMultiplier": item.upgrade_multiplier,
            "obtained_at": item.obtained_at.isoformat() if item.obtained_at else None
        })
    
    # Point 1: Get count of invited friends
    invited_friends_count = db.query(User).filter(User.referred_by_id == user_id).count()
    
    return jsonify({
        "id": user.id, "username": user.username, "first_name": user.first_name,
        "last_name": user.last_name, "tonBalance": user.ton_balance,
        "starBalance": user.star_balance, "inventory": inventory_data,
        "referralCode": user.referral_code,
        "referralEarningsPending": user.referral_earnings_pending,
        "total_won_ton": user.total_won_ton,
        "invited_friends_count": invited_friends_count # Point 1
    })

# 🔴🔴🔴 PASTE YOUR `open_case_api` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
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
    # case_cost_stars = target_case.get('priceStars', 0) # If you implement star payments

    if user.ton_balance < case_cost_ton:
        return jsonify({"error": f"Not enough TON. Need {case_cost_ton}, have {user.ton_balance:.2f}"}), 400
    # if user.star_balance < case_cost_stars:
    #     return jsonify({"error": "Not enough stars"}), 400
    
    prizes = target_case.get('prizes', [])
    if not prizes: return jsonify({"error": "No prizes in this case"}), 500

    # Determine winner based on probabilities
    total_probability = sum(p.get('probability', 0) for p in prizes)
    winner_data = None

    if total_probability == 0 and prizes: # If no probabilities, pick one at random (equal chance)
        winner_data = random.choice(prizes)
    elif total_probability > 0:
        normalized_prizes = []
        # Normalize probabilities if they don't sum to 1 (or close to it)
        if abs(total_probability - 1.0) > 0.0001: 
            logger.warning(f"Probabilities for case {case_id} do not sum to 1 (sum={total_probability}). Normalizing.")
            for p_info in prizes:
                normalized_prizes.append({**p_info, 'probability': p_info.get('probability',0) / total_probability})
        else:
            normalized_prizes = prizes
        
        rand_val = random.random() # 0.0 to < 1.0
        current_prob_sum = 0
        for prize_info in normalized_prizes:
            current_prob_sum += prize_info.get('probability', 0)
            if rand_val <= current_prob_sum:
                winner_data = prize_info
                break
        if not winner_data: # Fallback if float precision issues, pick last or random from normalized
            winner_data = random.choice(normalized_prizes) 
    else: # No prizes or all probabilities are zero
        return jsonify({"error": "Case prize configuration error"}), 500
    
    if not winner_data: return jsonify({"error": "Could not determine prize"}), 500

    # Deduct cost and add prize to inventory
    user.ton_balance -= case_cost_ton
    # user.star_balance -= case_cost_stars
    user.total_won_ton += winner_data['floorPrice'] # Add prize value to total won

    db_nft = db.query(NFT).filter(NFT.name == winner_data['name']).first()
    if not db_nft:
        # This should ideally not happen if populate_initial_nfts_from_cases ran correctly
        logger.error(f"NFT '{winner_data['name']}' NOT FOUND in DB. This is unexpected. Creating on-the-fly.")
        image_fn_winner = winner_data.get('imageFilename', generate_image_filename_from_name(winner_data['name']))
        db_nft = NFT(name=winner_data['name'], image_filename=image_fn_winner, floor_price=winner_data['floorPrice'])
        db.add(db_nft)
        try:
            db.commit(); db.refresh(db_nft)
            logger.info(f"NFT '{winner_data['name']}' created on-the-fly during case open.")
        except Exception as e_create_nft:
            db.rollback() # Rollback NFT creation and balance changes
            user.ton_balance += case_cost_ton; user.total_won_ton -= winner_data['floorPrice']
            db.commit() # Commit the rollback of balance changes
            logger.error(f"Failed to create NFT '{winner_data['name']}' on-the-fly: {e_create_nft}")
            return jsonify({"error": "Internal prize data error, NFT creation failed"}), 500

    new_item = InventoryItem(
        user_id=user.id, nft_id=db_nft.id,
        current_value=db_nft.floor_price, # Initial value is floor price
        upgrade_multiplier=1.0 
    )
    db.add(new_item)
    db.commit()
    db.refresh(new_item) # Get the new_item.id
    
    return jsonify({
        "status": "success",
        "won_prize": { # Send back data for the frontend to display
            "id": new_item.id, # Crucial: ID of the new InventoryItem instance
            "name": db_nft.name, 
            "imageFilename": db_nft.image_filename,
            "floorPrice": db_nft.floor_price,
            "currentValue": new_item.current_value # Should be same as floorPrice initially
        },
        "new_balance_ton": user.ton_balance,
        # "new_balance_stars": user.star_balance,
    })

# 🔴🔴🔴 PASTE YOUR `upgrade_item_api` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    inventory_item_id = data.get('inventory_item_id')
    multiplier_str = data.get('multiplier_str') # Expecting string like "1.5", "2", etc.

    if not all([inventory_item_id, multiplier_str]):
        return jsonify({"error": "inventory_item_id and multiplier_str are required"}), 400
    
    try:
        multiplier = float(multiplier_str) 
        inventory_item_id = int(inventory_item_id)
    except ValueError:
        return jsonify({"error": "Invalid data format for multiplier or item_id"}), 400

    # Define upgrade chances on the backend to prevent client-side manipulation
    upgrade_chances_map = {1.5: 50, 2.0: 35, 3.0: 25, 5.0: 15, 10.0: 8, 20.0: 3} 
    if multiplier not in upgrade_chances_map:
        return jsonify({"error": f"Invalid multiplier: {multiplier}. Valid are {list(upgrade_chances_map.keys())}" }), 400
    
    success_chance = upgrade_chances_map[multiplier]

    db = next(get_db())
    item_to_upgrade = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()

    if not item_to_upgrade:
        return jsonify({"error": "Item not found in your inventory"}), 404

    if random.uniform(0, 100) < success_chance:
        # Upgrade success
        original_value = item_to_upgrade.current_value
        new_value = round(original_value * multiplier, 2) # Calculate new value
        value_increase = new_value - original_value # How much value was added
        
        item_to_upgrade.current_value = new_value
        item_to_upgrade.upgrade_multiplier *= multiplier # Track cumulative multiplier
        
        user = db.query(User).filter(User.id == user_id).first() # Re-fetch user for total_won_ton
        if user: user.total_won_ton += value_increase # Add only the increase in value
        
        db.commit()
        return jsonify({
            "status": "success", 
            "message": f"Upgrade successful! New value: {item_to_upgrade.current_value:.2f} TON",
            "item": { # Send back updated item details
                "id": item_to_upgrade.id, 
                "currentValue": item_to_upgrade.current_value, 
                "name": item_to_upgrade.nft.name, # Name doesn't change on upgrade unless you want it to
                "upgradeMultiplier": item_to_upgrade.upgrade_multiplier 
            }
        })
    else:
        # Upgrade failed - item is lost
        item_name_lost = item_to_upgrade.nft.name
        lost_value = item_to_upgrade.current_value # The value of the item that was lost
        
        user = db.query(User).filter(User.id == user_id).first() # Re-fetch user
        if user: user.total_won_ton -= lost_value # Subtract the lost value from total winnings
        
        db.delete(item_to_upgrade)
        db.commit()
        logger.info(f"Item {item_name_lost} (ID: {inventory_item_id}, Value: {lost_value}) from user {user_id} lost in upgrade.")
        return jsonify({
            "status": "failed", 
            "message": f"Upgrade failed! You lost {item_name_lost}.",
            "item_lost": True # Explicit flag for frontend
        })

# 🔴🔴🔴 PASTE YOUR `convert_to_ton_api` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
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

    conversion_value = item_to_convert.current_value # Use current_value which reflects upgrades
    user.ton_balance += conversion_value
    # When converting, the item's value is realized, but it's not "won" again.
    # total_won_ton reflects value obtained from cases/upgrades. Selling/converting is just changing form.
    # However, if an upgrade increased its value, that increase was already added to total_won_ton.
    # So, no change to total_won_ton here.
    
    db.delete(item_to_convert)
    db.commit()

    return jsonify({
        "status": "success",
        "message": f"{item_to_convert.nft.name} converted to {conversion_value:.2f} TON.",
        "new_balance_ton": user.ton_balance
    })

# 🔴🔴🔴 PASTE YOUR `sell_all_items_api` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404

    if not user.inventory: # Check if inventory is empty
        return jsonify({"status": "no_items", "message": "Inventory is empty."})

    total_sell_value = sum(item.current_value for item in user.inventory)
    user.ton_balance += total_sell_value
    # Similar to convert_to_ton, selling all items realizes their current value.
    # The value (and increases from upgrades) was already accounted for in total_won_ton.
    # No change to total_won_ton here.
    
    # Delete all items from inventory for this user
    for item in user.inventory: # user.inventory will be a list here due to relationship
        db.delete(item)
    db.commit()

    return jsonify({
        "status": "success",
        "message": f"All items sold for {total_sell_value:.2f} TON.",
        "new_balance_ton": user.ton_balance
    })


# 🔴🔴🔴 PASTE YOUR `initiate_deposit_api`, `check_blockchain_for_deposit`, `verify_deposit_api` FUNCTIONS HERE (UNCHANGED) 🔴🔴🔴
@app.route('/api/initiate_deposit', methods=['POST'])
def initiate_deposit_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Authentication failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    amount_str = data.get('amount')
    if amount_str is None: return jsonify({"error": "Amount is required"}), 400
    
    try:
        original_amount_ton = float(amount_str)
    except ValueError:
        return jsonify({"error": "Invalid amount format"}), 400
    
    if original_amount_ton <= 0: 
        return jsonify({"error": "Amount must be positive"}), 400
    if original_amount_ton > 10000: # Example max limit
        return jsonify({"error": "Maximum deposit amount exceeded"}), 400

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404

    # Generate a unique small addition to make the final amount somewhat unique for tracking
    unique_nano_part = random.randint(10000, 999999) # e.g., 0.00001 to 0.000999 TON
    original_amount_nano_ton = int(original_amount_ton * (10**9))
    final_amount_nano_ton_for_link_and_check = original_amount_nano_ton + unique_nano_part

    # Check for existing active pending deposit for this user
    existing_pending = db.query(PendingDeposit).filter(
        PendingDeposit.user_id == user_id,
        PendingDeposit.status == 'pending',
        PendingDeposit.expires_at > dt.now(timezone.utc)
    ).first()

    if existing_pending:
        time_left = existing_pending.expires_at - dt.now(timezone.utc)
        return jsonify({
            "error": "You already have an active deposit request.", # More specific error
            "message": f"Please complete your previous deposit or wait {int(time_left.total_seconds() / 60)} minutes for it to expire."
        }), 409 # HTTP 409 Conflict

    pending_deposit = PendingDeposit(
        user_id=user_id,
        original_amount_ton=original_amount_ton,
        unique_identifier_nano_ton=unique_nano_part, # Store the random part for reconciliation if needed
        final_amount_nano_ton=final_amount_nano_ton_for_link_and_check, # Store the exact nanoTON amount
        expected_comment=DEPOSIT_COMMENT, # Use the defined constant
        status='pending',
        expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES)
    )
    db.add(pending_deposit)
    db.commit()
    db.refresh(pending_deposit)

    # For display on frontend, convert nanoTON back to TON string, preserving precision
    amount_to_send_str_display = f"{final_amount_nano_ton_for_link_and_check / (10**9):.9f}".rstrip('0').rstrip('.') 

    logger.info(f"Initiated deposit for user {user_id}: ID {pending_deposit.id}, AmountForLink {final_amount_nano_ton_for_link_and_check} nanoTON, Orig {original_amount_ton} TON, Comment: {DEPOSIT_COMMENT}")

    return jsonify({
        "status": "success",
        "pending_deposit_id": pending_deposit.id,
        "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW,
        "amount_to_send": amount_to_send_str_display, # For display if needed, but link uses nanoTON
        "final_amount_nano_ton": final_amount_nano_ton_for_link_and_check, # Crucial for ton:// link
        "comment": DEPOSIT_COMMENT, # URL encoded by frontend if necessary
        "expires_at": pending_deposit.expires_at.isoformat()
    })
async def check_blockchain_for_deposit(pending_deposit: PendingDeposit, db_session): # Pass db_session
    logger.info(f"Checking blockchain for pending deposit ID: {pending_deposit.id}, User: {pending_deposit.user_id}, Amount: {pending_deposit.final_amount_nano_ton} nanoTON, Comment: '{pending_deposit.expected_comment}'")
    
    provider = None # Initialize provider to None
    transaction_found_and_processed = False

    try:
        # Ensure LiteBalancer is configured correctly (e.g., from mainnet or testnet config)
        # For mainnet:
        provider = LiteBalancer.from_mainnet_config(trust_level=2) 
        # For testnet (if you are testing there):
        # provider = LiteBalancer.from_testnet_config(trust_level=2)
        
        await provider.start_up()
        logger.info(f"LiteBalancer started up successfully for deposit check {pending_deposit.id}")

        # Fetch recent transactions for the deposit recipient address
        # Adjust 'count' as needed; too low might miss tx, too high is less efficient.
        transactions = await provider.get_transactions(address=DEPOSIT_RECIPIENT_ADDRESS_RAW, count=30) 
        logger.info(f"Fetched {len(transactions)} transactions for address {DEPOSIT_RECIPIENT_ADDRESS_RAW}")

        for tx_data in transactions:
            # We are interested in incoming internal messages (transfers)
            if not tx_data.in_msg or not tx_data.in_msg.is_internal:
                continue

            tx_value_nano = tx_data.in_msg.info.value_coins # Value in nanoTON
            tx_comment_text = None
            tx_hash_hex = tx_data.cell.hash.hex() # Unique hash of the transaction

            # Optimization: Skip transactions older than the pending deposit creation time (with some buffer)
            if tx_data.now < int((pending_deposit.created_at - timedelta(minutes=5)).timestamp()):
                continue
            
            # Attempt to parse comment from the message body
            body_cell_slice = tx_data.in_msg.body.begin_parse()
            # Standard comment format: first 32 bits are op_code (0 for text comment)
            if body_cell_slice.remaining_bits >= 32: # Check if there are enough bits for op_code
                op_code = body_cell_slice.load_uint(32)
                if op_code == 0: # 0 indicates a text comment follows
                    try:
                        tx_comment_text = body_cell_slice.load_snake_string()
                    except Exception as e_parse_comment:
                        # Not all messages with op_code 0 will have a parsable string comment
                        logger.debug(f"Could not parse comment from transaction {tx_hash_hex} with op_code 0: {e_parse_comment}")
            else:
                logger.debug(f"Transaction {tx_hash_hex} body too short for op_code.")
            
            logger.debug(f"Scanning TX: hash={tx_hash_hex}, val={tx_value_nano}, cmt='{tx_comment_text}' (Expected val: {pending_deposit.final_amount_nano_ton}, cmt: '{pending_deposit.expected_comment}')")

            # Check for match: exact amount and exact comment
            if tx_value_nano == pending_deposit.final_amount_nano_ton and \
               tx_comment_text == pending_deposit.expected_comment:
                
                logger.info(f"MATCH FOUND for deposit ID {pending_deposit.id}! TX hash: {tx_hash_hex}")
                
                # It's crucial this runs within the original db_session context for commit
                user_to_credit = db_session.query(User).filter(User.id == pending_deposit.user_id).first()
                if not user_to_credit:
                    logger.error(f"Critical: User {pending_deposit.user_id} for pending deposit {pending_deposit.id} not found for crediting!")
                    pending_deposit.status = 'failed' # Mark as failed if user vanished
                    db_session.commit()
                    transaction_found_and_processed = True # Stop further checks
                    break # Exit loop, this deposit is resolved (as failed)

                user_to_credit.ton_balance += pending_deposit.original_amount_ton
                
                # Apply referral bonus if applicable
                if user_to_credit.referred_by_id:
                    referrer = db_session.query(User).filter(User.id == user_to_credit.referred_by_id).first()
                    if referrer:
                        referral_bonus = round(pending_deposit.original_amount_ton * 0.10, 2) # 10%
                        referrer.referral_earnings_pending += referral_bonus
                        logger.info(f"Referral bonus {referral_bonus:.2f} TON credited to referrer {referrer.id} from deposit by {user_to_credit.id}")

                pending_deposit.status = 'completed'
                db_session.commit()
                logger.info(f"Deposit ID {pending_deposit.id} completed for user {user_to_credit.id}. New balance: {user_to_credit.ton_balance:.2f}")
                transaction_found_and_processed = True
                return {"status": "success", "message": "Deposit confirmed and balance updated!", "new_balance_ton": user_to_credit.ton_balance}
        
        # If loop finishes and no transaction was found and processed
        if not transaction_found_and_processed:
            logger.info(f"No matching transaction found yet for deposit ID {pending_deposit.id}.")
            # Check for expiry after checking all recent transactions
            if pending_deposit.expires_at <= dt.now(timezone.utc):
                if pending_deposit.status == 'pending': # Ensure we only mark as expired once
                    pending_deposit.status = 'expired'
                    db_session.commit()
                logger.info(f"Deposit ID {pending_deposit.id} has expired.")
                return {"status": "expired", "message": "This deposit request has expired."}
            # If not expired and not found, it's still pending
            return {"status": "pending", "message": "Transaction not yet confirmed on the blockchain. Please wait a few minutes and try again."}

    except ConnectionRefusedError: # Specific error for LiteBalancer connection issues
        logger.error(f"LiteBalancer connection refused for deposit ID {pending_deposit.id}. Check network or LiteServer availability.")
        return {"status": "error", "message": "Cannot connect to TON network to verify. Please try again later."}
    except Exception as e:
        logger.error(f"Error during blockchain check for deposit ID {pending_deposit.id}: {type(e).__name__} - {e}", exc_info=True)
        # Generic error for other issues (pytoniq errors, etc.)
        return {"status": "error", "message": "An error occurred while checking for your transaction. Please try again later."}
    finally:
        if provider: 
            try:
                await provider.close_all()
                logger.info(f"LiteBalancer closed for deposit check {pending_deposit.id}")
            except Exception as close_e:
                logger.error(f"Error closing LiteBalancer for deposit check {pending_deposit.id}: {close_e}")
        else:
            logger.warning(f"LiteBalancer was not initialized for deposit check {pending_deposit.id}, no close needed.")
        logger.info(f"Blockchain check finished for deposit ID {pending_deposit.id}")
@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Authentication failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    pending_deposit_id = data.get('pending_deposit_id')

    if not pending_deposit_id:
        return jsonify({"error": "pending_deposit_id is required"}), 400
    
    db = next(get_db()) # Get a new session for this request
    pending_deposit = db.query(PendingDeposit).filter(
        PendingDeposit.id == pending_deposit_id,
        PendingDeposit.user_id == user_id # Ensure user owns this pending deposit
    ).first()

    if not pending_deposit:
        return jsonify({"error": "Pending deposit request not found or does not belong to you."}), 404

    # If already completed, just return success
    if pending_deposit.status == 'completed':
        user = db.query(User).filter(User.id == user_id).first() # Re-fetch user for current balance
        return jsonify({"status": "success", "message": "This deposit has already been confirmed.", "new_balance_ton": user.ton_balance if user else 0})
    
    # If expired, mark it if it was still pending and inform user
    if pending_deposit.status == 'expired' or pending_deposit.expires_at <= dt.now(timezone.utc):
        if pending_deposit.status == 'pending': # Mark as expired if checked after expiry time
            pending_deposit.status = 'expired'
            db.commit()
        return jsonify({"status": "expired", "message": "This deposit request has expired. Please initiate a new one."}), 400 # 400 or custom code
    
    # If still pending, run the blockchain check
    # This needs to run in an asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        # Pass the current db session to the async function
        result = loop.run_until_complete(check_blockchain_for_deposit(pending_deposit, db))
    finally:
        loop.close()
    
    return jsonify(result) # Return the result from check_blockchain_for_deposit

# 🔴🔴🔴 PASTE YOUR `get_leaderboard_api` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    # Query users, order by total_won_ton in descending order, limit to top 100
    leaders_query = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
    
    leaderboard_data = []
    for rank, user_leader in enumerate(leaders_query, 1):
        # Determine a display name: use first_name, then username, then a generic User_ID
        display_name = user_leader.first_name
        if not display_name and user_leader.username:
            display_name = user_leader.username
        if not display_name:
            display_name = f"User_{str(user_leader.id)[:6]}" # Truncated ID for brevity

        # Determine avatar character: first letter of display_name or 'U'
        avatar_char = (display_name[0] if display_name else "U").upper()

        leaderboard_data.append({
            "rank": rank,
            "name": display_name,
            "avatarChar": avatar_char,
            "income": user_leader.total_won_ton, # This is the score metric
            "user_id": user_leader.id # Include user_id if frontend needs to highlight current user
        })
    return jsonify(leaderboard_data)

# 🔴🔴🔴 PASTE YOUR `withdraw_referral_earnings_api` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
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
        user.referral_earnings_pending = 0.0 # Reset pending earnings
        db.commit()
        return jsonify({
            "status": "success",
            "message": f"{amount_withdrawn:.2f} TON referral earnings withdrawn to main balance.",
            "new_balance_ton": user.ton_balance,
            "new_referral_earnings_pending": user.referral_earnings_pending
        })
    else:
        return jsonify({"status": "no_earnings", "message": "No referral earnings to withdraw."})


# Point 2: API Endpoint for Promocode Redemption
@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Authentication failed"}), 401
    user_id = auth_user_data["id"]

    data = flask_request.get_json()
    promocode_text = data.get('promocode_text', "").strip()

    if not promocode_text:
        return jsonify({"status": "error", "message": "Promocode cannot be empty."}), 400

    db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return jsonify({"status": "error", "message": "User not found."}), 404

    promo_code_entry = db.query(PromoCode).filter(PromoCode.code_text == promocode_text).first()

    if not promo_code_entry:
        return jsonify({"status": "error", "message": "Invalid promocode."}), 404
    
    if promo_code_entry.activations_left <= 0:
        return jsonify({"status": "error", "message": "This promocode has no activations left."}), 400

    # Optional: Check if this user has already activated THIS specific code type.
    # This would require the UserPromoCodeActivation table.
    # existing_activation = db.query(UserPromoCodeActivation).filter(
    #     UserPromoCodeActivation.user_id == user_id,
    #     UserPromoCodeActivation.promo_code_id == promo_code_entry.id
    # ).first()
    # if existing_activation:
    #     return jsonify({"status": "error", "message": "You have already used this promocode."}), 400

    # Proceed with activation
    promo_code_entry.activations_left -= 1
    user.ton_balance += promo_code_entry.ton_amount
    
    # Optional: Record the activation
    # new_activation = UserPromoCodeActivation(user_id=user_id, promo_code_id=promo_code_entry.id)
    # db.add(new_activation)
    
    try:
        db.commit()
        logger.info(f"User {user_id} redeemed promocode '{promocode_text}'. Received {promo_code_entry.ton_amount} TON. Activations left: {promo_code_entry.activations_left}")
        return jsonify({
            "status": "success",
            "message": f"Promocode '{promocode_text}' redeemed! You received {promo_code_entry.ton_amount:.2f} TON.",
            "new_balance_ton": user.ton_balance
        })
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Database error redeeming promocode for user {user_id}: {e}")
        return jsonify({"status": "error", "message": "Database error. Please try again."}), 500


# Point 3: API Endpoint to finalize withdrawal (remove item from DB)
@app.route('/api/finalize_withdrawal/<int:inventory_item_id>', methods=['POST'])
def finalize_withdrawal_api(inventory_item_id):
    init_data_str = flask_request.headers.get('X-Telegram-Init-Data')
    auth_user_data = validate_init_data(init_data_str, BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Authentication failed"}), 401
    user_id = auth_user_data["id"]

    db = next(get_db())
    item_to_remove = db.query(InventoryItem).filter(
        InventoryItem.id == inventory_item_id,
        InventoryItem.user_id == user_id
    ).first()

    if not item_to_remove:
        return jsonify({"status": "error", "message": "Item not found in your inventory or already withdrawn."}), 404

    item_name = item_to_remove.nft.name
    item_value = item_to_remove.current_value

    # Before deleting, adjust total_won_ton if the withdrawal implies "losing" its value from the game's economy perspective.
    # If withdrawal means the item is out of the game, its value should be subtracted from total_won_ton.
    user = db.query(User).filter(User.id == user_id).first()
    if user:
        user.total_won_ton -= item_value 
        # Ensure total_won_ton doesn't go negative if something is off
        user.total_won_ton = max(0, user.total_won_ton) 
    
    db.delete(item_to_remove)
    
    try:
        db.commit()
        logger.info(f"User {user_id} finalized withdrawal of item ID {inventory_item_id} ('{item_name}', value: {item_value}). Item removed from DB. Total won adjusted.")
        return jsonify({"status": "success", "message": f"Withdrawal of '{item_name}' confirmed."})
    except SQLAlchemyError as e:
        db.rollback()
        # If rollback, revert total_won_ton adjustment if it was made
        if user: user.total_won_ton += item_value 
        # Don't commit here, just log the state
        logger.error(f"Database error finalizing withdrawal for item {inventory_item_id}, user {user_id}: {e}")
        return jsonify({"status": "error", "message": "Database error during withdrawal finalization."}), 500


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
            referral_code=f"ref_{message.chat.id}_{random.randint(1000,9999)}" # Generate a unique referral code
        )
        db.add(user) 
    
    try:
        start_param = message.text.split(' ')
        if len(start_param) > 1 and start_param[1].startswith('ref_'):
            referrer_code_param = start_param[1]
            # Apply referral only if the user is being created now AND doesn't already have a referrer
            if created_now and not user.referred_by_id: 
                referrer = db.query(User).filter(User.referral_code == referrer_code_param).first()
                if referrer and referrer.id != user.id : # Cannot refer self
                    user.referred_by_id = referrer.id
                    logger.info(f"Пользователь {user.id} пришел по реф. коду {referrer_code_param} от {referrer.id}")
                    # Optionally, send a notification to the referrer
                    try:
                        bot.send_message(referrer.id, f"🎉 Ваш друг {user.first_name or user.username or user.id} присоединился по вашей ссылке!")
                    except Exception as e_notify:
                        logger.warning(f"Не удалось уведомить реферера {referrer.id}: {e_notify}")
    except Exception as e:
        logger.error(f"Ошибка обработки реферального параметра для {user.id}: {e}")

    # Update user details if they changed in Telegram profile
    changed_in_db = False
    if user.username != message.from_user.username: user.username = message.from_user.username; changed_in_db=True
    if user.first_name != message.from_user.first_name: user.first_name = message.from_user.first_name; changed_in_db=True
    if user.last_name != message.from_user.last_name: user.last_name = message.from_user.last_name; changed_in_db=True
    
    if created_now or changed_in_db:
        try:
            db.commit()
            if created_now: logger.info(f"Новый пользователь {message.chat.id} ({message.from_user.username or 'N/A'}) добавлен/обновлен в БД.")
            elif changed_in_db: logger.info(f"Данные пользователя {message.chat.id} обновлены.")
        except IntegrityError: # Could happen if referral code was not unique by cosmic chance
            db.rollback()
            logger.error(f"IntegrityError при сохранении пользователя {message.chat.id}. Возможно, дублирующийся referral_code.")
            # Try to regenerate referral code if it was the issue
            if created_now and user.referral_code.startswith(f"ref_{message.chat.id}_"):
                user.referral_code = f"ref_{message.chat.id}_{random.randint(10000,99999)}" # Wider range
                try:
                    db.add(user); db.commit()
                    logger.info(f"Referral code regenerated for user {message.chat.id}")
                except Exception as e_retry:
                    db.rollback()
                    logger.error(f"Failed to save user {message.chat.id} even after regenerating referral code: {e_retry}")
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
        app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app_info)
        markup.add(app_button)
        bot.send_message(
            message.chat.id,
            "Добро пожаловать в Pusik Gifts! 🎁\n\n"
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
    bot.reply_to(message, "Нажмите /start, чтобы открыть Pusik Gifts.")

# --- Polling ---
# 🔴🔴🔴 PASTE YOUR `run_bot_polling` FUNCTION HERE (UNCHANGED) 🔴🔴🔴
bot_polling_started = False
bot_polling_thread = None
def run_bot_polling():
    global bot_polling_started
    if bot_polling_started: 
        logger.info("Polling уже запущен.")
        return
    
    bot_polling_started = True
    logger.info("Запуск бота в режиме polling...")
    
    max_retries_remove_webhook = 3
    for i in range(max_retries_remove_webhook):
        try:
            bot.remove_webhook()
            logger.info("Вебхук успешно удален (если был).")
            break # Success
        except Exception as e:
            logger.warning(f"Попытка {i+1}/{max_retries_remove_webhook} удалить вебхук не удалась: {e}")
            if i < max_retries_remove_webhook - 1:
                time.sleep(2) # Wait before retrying
            else:
                logger.error("Не удалось удалить вебхук после нескольких попыток. Polling может не работать корректно если вебхук активен.")
    
    while True: # Outer loop for restarting infinity_polling if it crashes badly
        if not bot_polling_started: # Check flag in case polling was stopped externally
            logger.info("Polling остановлен.")
            break
        try:
            logger.info("Старт infinity_polling...")
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
            # If infinity_polling exits cleanly (e.g., stop_polling called), it won't raise an exception
            logger.info("infinity_polling завершился без ошибок.")
            # Depending on desired behavior, you might want to break here or continue to restart
            # For robustness, let's assume we want it to keep running unless explicitly stopped.
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"Ошибка API Telegram в polling: {e}. Код: {e.error_code}")
            if e.error_code == 401: # Unauthorized
                logger.error("Неверный токен бота. Polling остановлен.")
                bot_polling_started = False # Stop trying
                break 
            elif e.error_code == 409: # Conflict (e.g., webhook set elsewhere)
                logger.error("Конфликт (возможно, другой экземпляр бота запущен или вебхук установлен). Polling остановлен.")
                bot_polling_started = False # Stop trying
                break
            else:
                logger.error(f"Другая ошибка API Telegram, перезапуск polling через 30 секунд...")
                time.sleep(30)
        except ConnectionError as e: # Includes requests.exceptions.ConnectionError
            logger.error(f"Ошибка соединения: {e}. Перезапуск polling через 60 секунд...")
            time.sleep(60)
        except Exception as e: # Catch-all for other unexpected errors
            logger.error(f"Критическая ошибка в polling: {type(e).__name__} - {e}. Перезапуск polling через 60 секунд...", exc_info=True)
            time.sleep(60)
        else: # If infinity_polling finishes without exception (e.g. by bot.stop_polling())
            logger.warning("infinity_polling завершился штатно. Если это не ожидалось, проверьте. Перезапуск через 15 секунд...")
            time.sleep(15) # Wait a bit before restarting, in case it was a manual stop
        
        if not bot_polling_started: # Double check before looping again
            logger.info("Polling окончательно остановлен перед следующим циклом.")
            break


if __name__ == '__main__':
    if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        logger.info("Основной процесс, запуск потока для polling бота.")
        bot_polling_thread = threading.Thread(target=run_bot_polling)
        bot_polling_thread.daemon = True # Allow main thread to exit even if polling thread is running
        bot_polling_thread.start()
    elif os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        # This is the reloader process from Werkzeug, do not start polling here
        logger.info("Процесс Werkzeug reloader, polling бота не запускается здесь.")
    
    logger.info("Запуск Flask development server...")
    # use_reloader=False if you face issues with multiple polling threads in dev. For prod, this is fine.
    # Debug=False for production.
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
