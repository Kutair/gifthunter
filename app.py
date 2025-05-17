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

# Imports for Tonnel Withdrawal
from curl_cffi.requests import AsyncSession
from javascript import require # Ensure python-javascript is installed
# crypto_js_object = require("crypto-js") # Loaded globally or within Tonnel class

# Pytoniq imports
from pytoniq import LiteBalancer
import asyncio


load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_NAME = os.environ.get("MINI_APP_NAME", "case")
MINI_APP_URL = os.environ.get("MINI_APP_URL", f"https://t.me/caseKviBot/{MINI_APP_NAME}")
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24
TONNEL_SENDER_INIT_DATA = os.environ.get("TONNEL_SENDER_INIT_DATA") # CRITICAL: For Tonnel class
TONNEL_GIFT_SECRET = os.environ.get("TONNEL_GIFT_SECRET", "yowtfisthispieceofshitiiit") # The hardcoded secret

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
if not TONNEL_SENDER_INIT_DATA:
    logger.warning("TONNEL_SENDER_INIT_DATA is not set! Tonnel gift withdrawal will likely fail.")


engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Database Models (User, NFT, InventoryItem, PendingDeposit, PromoCode) ---
# (Keep your existing model definitions here - they were correct in the previous response)
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

# --- Tonnel Gift Withdrawal Class ---
class TonnelGiftSender:
    def __init__(self, sender_auth_data: str, gift_secret: str):
        self.session = AsyncSession(impersonate="chrome110", http_version=2) # Using impersonate for better header mimicry
        self.secret = gift_secret
        self.authdata = sender_auth_data # This MUST be the initData of the account PAYING for/SENDING the gift
        self._crypto_js = require("crypto-js") # Load crypto-js

    async def _make_request(self, method, url, headers=None, json_payload=None, timeout=30):
        try:
            if method.upper() == "GET":
                response = await self.session.get(url, headers=headers, timeout=timeout)
            elif method.upper() == "POST":
                response = await self.session.post(url, headers=headers, json=json_payload, timeout=timeout)
            elif method.upper() == "OPTIONS":
                response = await self.session.options(url, headers=headers, timeout=timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            logger.debug(f"Tonnel API {method} {url} - Status: {response.status_code}")
            response.raise_for_status() # Will raise an exception for 4xx/5xx errors
            if response.status_code == 204: # No content
                return None
            return response.json()
        except Exception as e:
            logger.error(f"Tonnel API request error ({method} {url}): {e}", exc_info=True)
            # Try to get response text if available for more context
            try: logger.error(f"Response content: {response.text}")
            except: pass
            raise # Re-raise the exception to be handled by the caller

    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int):
        logger.info(f"Attempting to send Tonnel gift '{gift_item_name}' to user_id {receiver_telegram_id} using sender auth starting with: {self.authdata[:30]}...")

        if not self.authdata:
            logger.error("Sender authData (TONNEL_SENDER_INIT_DATA) is not configured. Cannot proceed with Tonnel gift.")
            return {"status": "error", "message": "Tonnel sender not configured on backend."}

        try:
            # 1. Initial GET to marketplace (cookie/session setup)
            # Headers are simplified here, curl_cffi's impersonate handles many common ones
            await self._make_request("GET", "https://marketplace.tonnel.network/")
            logger.info("Tonnel: Initial GET to marketplace.tonnel.network successful.")
            
            # 2. Get available gifts for the item name
            filter_str = json.dumps({ # Use json.dumps for correct JSON string
                "price": {"$exists": True},
                "refunded": {"$ne": True},
                "buyer": {"$exists": False},
                "export_at": {"$exists": True},
                "gift_name": gift_item_name,
                "asset": "TON"
            })
            
            page_gifts_payload = {
                "filter": filter_str, "limit": 10, "page": 1, "price_range": "", "ref": 0,
                "sort": '{"price":1,"gift_id":-1}', "user_auth": "" # Unclear what user_auth is for here
            }
            post_headers_page_gifts = { # Kept specific headers as they might be crucial
                "Content-Type": "application/json",
                "Origin": "https://marketplace.tonnel.network",
                "Referer": "https://marketplace.tonnel.network/",
            }
            
            # Pre-flight OPTIONS for pageGifts
            await self._make_request("OPTIONS", "https://gifts2.tonnel.network/api/pageGifts", headers={"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type", "Origin": "https://tonnel-gift.vercel.app", "Referer": "https://tonnel-gift.vercel.app/"})

            gifts_found = await self._make_request("POST", "https://gifts2.tonnel.network/api/pageGifts", headers=post_headers_page_gifts, json_payload=page_gifts_payload)
            
            if not gifts_found or not isinstance(gifts_found, list) or len(gifts_found) == 0:
                logger.warning(f"Tonnel: No gifts found on marketplace for '{gift_item_name}'. Response: {gifts_found}")
                return {"status": "error", "message": f"No '{gift_item_name}' gifts currently available on Tonnel marketplace."}
            
            low_gift = gifts_found[0] # Cheapest one
            logger.info(f"Tonnel: Found cheapest gift for '{gift_item_name}': ID {low_gift.get('gift_id')}, Price {low_gift.get('price')} TON")

            # 3. UserInfo check (This uses self.authdata of the SENDER, and receiver_telegram_id for 'user' field in payload)
            # The purpose of this call with receiver_telegram_id in 'user' field is unclear without API docs.
            # It might be checking if the receiver is a valid Tonnel user or some other pre-condition.
            user_info_payload = {"authData": self.authdata, "user": receiver_telegram_id} # Using receiver_telegram_id here as in original script
            user_info_headers = {
                 "Content-Type": "application/json",
                 "Origin": "https://marketplace.tonnel.network",
                 "Referer": "https://marketplace.tonnel.network/",
            }
            # Pre-flight OPTIONS for userInfo might not be strictly needed if session cookies handle CORS after first OPTIONS, but for safety:
            await self._make_request("OPTIONS", "https://gifts2.tonnel.network/api/userInfo", headers={"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"})

            user_check_response = await self._make_request("POST", "https://gifts2.tonnel.network/api/userInfo", headers=user_info_headers, json_payload=user_info_payload)
            logger.info(f"Tonnel: UserInfo check response: {user_check_response}")

            if not user_check_response or user_check_response.get("status") != "success":
                logger.warning(f"Tonnel: UserInfo check failed or status not success for receiver {receiver_telegram_id}. Response: {user_check_response}")
                # Depending on Tonnel's API, this might mean the receiver isn't registered with Tonnel gifts.
                # The original script prints "Юзера нет в боте" - "User not in bot"
                return {"status": "error", "message": f"Tonnel user check failed for receiver. Details: {user_check_response.get('message', 'Unknown error')}"}

            # 4. Buy/Send Gift
            time_now = int(time.time())
            encrypted_timestamp = self._crypto_js.AES.encrypt(f"{time_now}", self.secret).toString()
            logger.debug(f"Tonnel: Encrypted timestamp generated: {encrypted_timestamp[:10]}...") # Log a snippet

            buy_gift_url = f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}" # Original script used gifts.coffin.meme
            buy_gift_payload = {
                "anonymously": True, "asset": "TON", "authData": self.authdata,
                "price": low_gift['price'], "receiver": receiver_telegram_id,
                "showPrice": False, "timestamp": encrypted_timestamp
            }
            buy_gift_headers = {
                "Content-Type": "application/json",
                "Origin": "https://marketplace.tonnel.network", # Referer from marketplace
                "Referer": "https://marketplace.tonnel.network/",
                "Host": "gifts.coffin.meme" # Explicit Host header
            }
            
            # Pre-flight OPTIONS for buyGift
            await self._make_request("OPTIONS", buy_gift_url, headers={"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"})
            
            purchase_response = await self._make_request("POST", buy_gift_url, headers=buy_gift_headers, json_payload=buy_gift_payload, timeout=90) # Longer timeout for purchase
            logger.info(f"Tonnel: BuyGift response for gift ID {low_gift['gift_id']} to user {receiver_telegram_id}: {purchase_response}")

            if purchase_response and purchase_response.get("status") == "success":
                logger.info(f"Tonnel: Successfully initiated gift transfer of '{gift_item_name}' to user {receiver_telegram_id}.")
                return {"status": "success", "message": f"Gift '{gift_item_name}' sent via Tonnel!", "details": purchase_response}
            else:
                logger.error(f"Tonnel: Failed to buy/send gift '{gift_item_name}'. Response: {purchase_response}")
                return {"status": "error", "message": f"Tonnel gift transfer failed. Details: {purchase_response.get('message', 'Unknown error')}"}

        except Exception as e:
            logger.error(f"Tonnel: Unexpected error during gift sending process for '{gift_item_name}' to {receiver_telegram_id}: {e}", exc_info=True)
            return {"status": "error", "message": f"An unexpected error occurred while processing Tonnel gift withdrawal: {str(e)}"}
        finally:
            await self.session.close()
            logger.info("Tonnel: AsyncSession closed.")


# --- Utility and Data Functions ---
def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png"
    name_str_replaced_hyphens = name_str.replace('-', '_')
    if name_str_replaced_hyphens in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']:
         return name_str_replaced_hyphens + '.png'
    cleaned_name = name_str.replace('&', 'and').replace("'", "").replace(" ", "-") # Simplified
    cleaned_name = re.sub(r'-+', '-', cleaned_name) # Replace multiple hyphens with one
    return cleaned_name + '.png'

UPDATED_FLOOR_PRICES = {
    'Plush Pepe': 1200.0, 'Neko Helmet': 15.0, 'Sharp Tongue': 17.0, "Durov's Cap": 251.0,
    'Voodoo Doll': 9.4, 'Vintage Cigar': 19.7, 'Astral Shard': 50.0, 'Scared Cat': 22.0,
    'Swiss Watch': 18.6, 'Perfume Bottle': 38.3, 'Precious Peach': 100.0, 'Toy Bear': 16.3,
    'Genie Lamp': 19.3, 'Loot Bag': 25.0, 'Kissed Frog': 14.8, 'Electric Skull': 10.9,
    'Diamond Ring': 8.06, 'Mini Oscar': 40.5, 'Party Sparkler': 2.0, 'Homemade Cake': 2.0, 
    'Cookie Heart': 1.8, 'Jack-in-the-box': 2.0, 'Skull Flower': 3.4, 'Lol Pop': 1.4, 
    'Hynpo Lollipop': 1.4, 'Desk Calendar': 1.4, 'B-Day Candle': 1.4, 'Record Player': 4.0, 
    'Jelly Bunny': 3.6, 'Tama Gadget': 4.0, 'Snow Globe': 4.0, 'Eternal Rose': 11.0, 
    'Love Potion': 5.4, 'Top Hat': 6.0
}

cases_data_backend = [
    { 'id': 'lolpop', 'name': 'Lol Pop Stash', 'priceTON': 1.5, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.001 }, { 'name': 'Neko Helmet', 'probability': 0.005 }, { 'name': 'Party Sparkler', 'probability': 0.07 }, { 'name': 'Homemade Cake', 'probability': 0.07 }, { 'name': 'Cookie Heart', 'probability': 0.07 }, { 'name': 'Jack-in-the-box', 'probability': 0.06 }, { 'name': 'Skull Flower', 'probability': 0.023 }, { 'name': 'Lol Pop', 'probability': 0.25 }, { 'name': 'Hynpo Lollipop', 'probability': 0.25 }, { 'name': 'Desk Calendar', 'probability': 0.10 }, { 'name': 'B-Day Candle', 'probability': 0.101 } ] },
    { 'id': 'recordplayer', 'name': 'Record Player Vault', 'priceTON': 6.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0012 }, { 'name': 'Record Player', 'probability': 0.40 }, { 'name': 'Lol Pop', 'probability': 0.10 }, { 'name': 'Hynpo Lollipop', 'probability': 0.10 }, { 'name': 'Party Sparkler', 'probability': 0.10 }, { 'name': 'Skull Flower', 'probability': 0.10 }, { 'name': 'Jelly Bunny', 'probability': 0.0988 }, { 'name': 'Tama Gadget', 'probability': 0.05 }, { 'name': 'Snow Globe', 'probability': 0.05 } ] },
    { 'id': 'swisswatch', 'name': 'Swiss Watch Box', 'priceTON': 10.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0015 }, { 'name': 'Swiss Watch', 'probability': 0.08 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Eternal Rose', 'probability': 0.05 }, { 'name': 'Electric Skull', 'probability': 0.03 }, { 'name': 'Diamond Ring', 'probability': 0.0395 }, { 'name': 'Record Player', 'probability': 0.20 }, { 'name': 'Love Potion', 'probability': 0.20 }, { 'name': 'Top Hat', 'probability': 0.15 }, { 'name': 'Voodoo Doll', 'probability': 0.149 } ] },
    { 'id': 'perfumebottle', 'name': 'Perfume Chest', 'priceTON': 20.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0018 }, { 'name': 'Perfume Bottle', 'probability': 0.08 }, { 'name': 'Sharp Tongue', 'probability': 0.12 }, { 'name': 'Loot Bag', 'probability': 0.09946 }, { 'name': 'Swiss Watch', 'probability': 0.15 }, { 'name': 'Neko Helmet', 'probability': 0.15 }, { 'name': 'Genie Lamp', 'probability': 0.15 }, { 'name': 'Kissed Frog', 'probability': 0.10 }, { 'name': 'Electric Skull', 'probability': 0.07 }, { 'name': 'Diamond Ring', 'probability': 0.07874 } ] },
    { 'id': 'vintagecigar', 'name': 'Vintage Cigar Safe', 'priceTON': 40.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.002 }, { 'name': 'Perfume Bottle', 'probability': 0.2994 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.12 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Sharp Tongue', 'probability': 0.10 }, { 'name': 'Genie Lamp', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.08 }, { 'name': 'Scared Cat', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.0486 } ] },
    { 'id': 'astralshard', 'name': 'Astral Shard Relic', 'priceTON': 100.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0025 }, { 'name': 'Durov\'s Cap', 'probability': 0.09925 }, { 'name': 'Astral Shard', 'probability': 0.10 }, { 'name': 'Precious Peach', 'probability': 0.10 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Perfume Bottle', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.10 }, { 'name': 'Neko Helmet', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.10 }, { 'name': 'Scared Cat', 'probability': 0.08 }, { 'name': 'Loot Bag', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.04825 } ] },
    { 'id': 'plushpepe', 'name': 'Plush Pepe Hoard', 'priceTON': 200.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.15 }, { 'name': 'Durov\'s Cap', 'probability': 0.25 }, { 'name': 'Astral Shard', 'probability': 0.60 } ] },
    { 'id': 'black', 'name': 'BLACK Singularity', 'isBackgroundCase': True, 'bgImageFilename': 'image-1.png', 'overlayPrizeName': 'Neko Helmet', 'priceTON': 30.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.001 }, { 'name': 'Durov\'s Cap', 'probability': 0.01 }, { 'name': 'Perfume Bottle', 'probability': 0.05 }, { 'name': 'Mini Oscar', 'probability': 0.04 }, { 'name': 'Scared Cat', 'probability': 0.06 }, { 'name': 'Vintage Cigar', 'probability': 0.07 }, { 'name': 'Loot Bag', 'probability': 0.07 }, { 'name': 'Sharp Tongue', 'probability': 0.08 }, { 'name': 'Genie Lamp', 'probability': 0.08 }, { 'name': 'Swiss Watch', 'probability': 0.10 }, { 'name': 'Neko Helmet', 'probability': 0.15 }, { 'name': 'Kissed Frog', 'probability': 0.10 }, { 'name': 'Electric Skull', 'probability': 0.09 }, { 'name': 'Diamond Ring', 'probability': 0.089} ] }
]
# Populate full prize data (floorPrice, imageFilename) into cases_data_backend
for case_dict in cases_data_backend:
    if not case_dict.get('isBackgroundCase') and 'name' in case_dict and 'imageFilename' not in case_dict : # Add imageFilename for non-bg cases if missing
        case_dict['imageFilename'] = generate_image_filename_from_name(case_dict['name'])
    for prize_stub in case_dict['prizes']:
        prize_name = prize_stub['name']
        prize_stub['imageFilename'] = generate_image_filename_from_name(prize_name)
        prize_stub['floorPrice'] = UPDATED_FLOOR_PRICES.get(prize_name, 0)


def populate_initial_data():
    db = SessionLocal()
    try:
        existing_nft_names = {name[0] for name in db.query(NFT.name).all()}
        nfts_to_add_or_update = []
        for prize_name_key, floor_price_val in UPDATED_FLOOR_PRICES.items():
            if prize_name_key not in existing_nft_names:
                nfts_to_add_or_update.append(NFT(name=prize_name_key, image_filename=generate_image_filename_from_name(prize_name_key), floor_price=floor_price_val))
                existing_nft_names.add(prize_name_key)
            else:
                nft_in_db = db.query(NFT).filter(NFT.name == prize_name_key).first()
                if nft_in_db and nft_in_db.floor_price != floor_price_val: nft_in_db.floor_price = floor_price_val
        if nfts_to_add_or_update: db.add_all(nfts_to_add_or_update)
        db.commit(); logger.info(f"Populated/Updated NFT data. New: {len(nfts_to_add_or_update)}.")
        if not db.query(PromoCode).filter(PromoCode.code_text == 'durov').first():
            db.add(PromoCode(code_text='durov', activations_left=10, ton_amount=5.0)); db.commit(); logger.info("Promocode 'durov' seeded.")
    except Exception as e: db.rollback(); logger.error(f"Error populate_initial_data: {e}", exc_info=True)
    finally: db.close()
populate_initial_data()

# --- Flask App and other helper functions (get_db, validate_init_data) ---
# (Keep these as they were in the previous full Python response)
DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR"; DEPOSIT_COMMENT = "cpd7r07ud3s"; PENDING_DEPOSIT_EXPIRY_MINUTES = 30
app = Flask(__name__); CORS(app, resources={r"/api/*": {"origins": ["https://vasiliy-katsyka.github.io"]}})
if not BOT_TOKEN: logger.error("BOT_TOKEN not found!"); exit("BOT_TOKEN is not set.")
bot = telebot.TeleBot(BOT_TOKEN)
def get_db(): db = SessionLocal();_ = (yield db);db.close()
def validate_init_data(init_data_str: str, bot_token: str) -> dict | None:
    try:
        if not init_data_str: return None
        parsed_data = dict(parse_qs(init_data_str))
        if not all(k in parsed_data for k in ['hash', 'user', 'auth_date']): return None
        hash_received = parsed_data.pop('hash')[0]; auth_date_ts = int(parsed_data['auth_date'][0])
        if (int(dt.now(timezone.utc).timestamp()) - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS: return None
        data_check_string = "\n".join(f"{k}={parsed_data[k][0]}" for k in sorted(parsed_data.keys()))
        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash_hex == hash_received:
            user_info_dict = json.loads(unquote(parsed_data['user'][0]))
            if 'id' not in user_info_dict: return None
            return { "id": int(user_info_dict["id"]), **user_info_dict } # Return all user fields
        return None
    except Exception as e: logger.error(f"initData validation error: {e}", exc_info=True); return None


# --- API Endpoints ---
@app.route('/')
def index_route(): return "Pusik Gifts App is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; db = next(get_db())
    user = db.query(User).filter(User.id == user_id).first()
    if not user: user = User(id=user_id, username=auth_user_data.get("username"), first_name=auth_user_data.get("first_name"), last_name=auth_user_data.get("last_name"), referral_code=f"ref_{user_id}_{random.randint(1000,9999)}"); db.add(user); db.commit(); db.refresh(user)
    inventory_data = [{"id": item.id, "name": item.nft.name, "imageFilename": item.nft.image_filename, "floorPrice": item.nft.floor_price, "currentValue": item.current_value, "upgradeMultiplier": item.upgrade_multiplier, "variant": item.variant, "obtained_at": item.obtained_at.isoformat() if item.obtained_at else None} for item in user.inventory]
    invited_friends_count = db.query(User).filter(User.referred_by_id == user_id).count()
    return jsonify({"id": user.id, "username": user.username, "first_name": user.first_name, "last_name": user.last_name, "tonBalance": user.ton_balance, "starBalance": user.star_balance, "inventory": inventory_data, "referralCode": user.referral_code, "referralEarningsPending": user.referral_earnings_pending, "total_won_ton": user.total_won_ton, "invited_friends_count": invited_friends_count})

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; data = flask_request.get_json(); case_id = data.get('case_id')
    if not case_id: return jsonify({"error": "case_id required"}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404
    target_case = next((c for c in cases_data_backend if c['id'] == case_id), None)
    if not target_case: return jsonify({"error": "Case not found"}), 404
    case_cost_ton = target_case['priceTON']
    if user.ton_balance < case_cost_ton: return jsonify({"error": f"Not enough TON. Need {case_cost_ton:.2f}"}), 400
    prizes = target_case['prizes']; rand_val = random.random(); current_prob_sum = 0; chosen_prize_info = None
    for p_info in prizes: current_prob_sum += p_info['probability'];
        if rand_val <= current_prob_sum: chosen_prize_info = p_info; break
    if not chosen_prize_info: chosen_prize_info = random.choice(prizes)
    user.ton_balance -= case_cost_ton
    db_nft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
    if not db_nft: user.ton_balance += case_cost_ton; db.commit(); return jsonify({"error": "Prize NFT missing"}), 500
    item_variant = "black_singularity" if target_case['id'] == 'black' else None
    actual_item_value = db_nft.floor_price * (2.5 if item_variant == "black_singularity" else 1)
    user.total_won_ton += actual_item_value
    new_item = InventoryItem(user_id=user.id, nft_id=db_nft.id, current_value=actual_item_value, variant=item_variant)
    db.add(new_item); db.commit(); db.refresh(new_item)
    return jsonify({"status": "success", "won_prize": {"id": new_item.id, "name": db_nft.name, "imageFilename": db_nft.image_filename, "floorPrice": db_nft.floor_price, "currentValue": new_item.current_value, "variant": new_item.variant}, "new_balance_ton": user.ton_balance})

# ... (Keep other API endpoints: upgrade, convert, sell, deposit, verify, leaderboard, referrals, promocode as they were)
@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; data = flask_request.get_json(); inventory_item_id = data.get('inventory_item_id'); multiplier_str = data.get('multiplier_str')
    if not all([inventory_item_id, multiplier_str]): return jsonify({"error": "Missing params"}), 400
    try: multiplier = float(multiplier_str); inventory_item_id = int(inventory_item_id)
    except ValueError: return jsonify({"error": "Invalid data format"}), 400
    upgrade_chances_map = {1.5: 50, 2.0: 35, 3.0: 25, 5.0: 15, 10.0: 8, 20.0: 3}
    if multiplier not in upgrade_chances_map: return jsonify({"error": "Invalid multiplier"}), 400
    db = next(get_db()); item_to_upgrade = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()
    if not item_to_upgrade: return jsonify({"error": "Item not found"}), 404
    user = db.query(User).filter(User.id == user_id).first()
    if random.uniform(0, 100) < upgrade_chances_map[multiplier]:
        original_value = item_to_upgrade.current_value; new_value = round(original_value * multiplier, 2); value_increase = new_value - original_value
        item_to_upgrade.current_value = new_value; item_to_upgrade.upgrade_multiplier *= multiplier
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
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; data = flask_request.get_json(); inventory_item_id = data.get('inventory_item_id')
    if not inventory_item_id: return jsonify({"error": "ID required"}), 400
    try: inventory_item_id = int(inventory_item_id)
    except ValueError: return jsonify({"error": "Invalid ID"}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == user_id).first(); item_to_convert = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == user_id).first()
    if not user or not item_to_convert: return jsonify({"error": "User/item not found"}), 404
    conversion_value = item_to_convert.current_value; user.ton_balance += conversion_value
    db.delete(item_to_convert); db.commit()
    return jsonify({"status": "success", "message": f"{item_to_convert.nft.name} converted for {conversion_value:.2f} TON.", "new_balance_ton": user.ton_balance})

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; db = next(get_db()); user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404
    if not user.inventory: return jsonify({"status": "no_items", "message": "Inventory empty."})
    total_sell_value = sum(item.current_value for item in user.inventory)
    user.ton_balance += total_sell_value;
    for item in list(user.inventory): db.delete(item)
    db.commit()
    return jsonify({"status": "success", "message": f"All sold for {total_sell_value:.2f} TON.", "new_balance_ton": user.ton_balance})

@app.route('/api/initiate_deposit', methods=['POST'])
def initiate_deposit_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
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
    display_amount = f"{final_nano / 1e9:.9f}".rstrip('0').rstrip('.');
    return jsonify({"status": "success", "pending_deposit_id": pending.id, "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW, "amount_to_send": display_amount, "final_amount_nano_ton": final_nano, "comment": DEPOSIT_COMMENT, "expires_at": pending.expires_at.isoformat()})

async def check_blockchain_for_deposit(pending_deposit: PendingDeposit, db_session):
    provider = None
    try:
        provider = LiteBalancer.from_mainnet_config(trust_level=2); await provider.start_up()
        txs = await provider.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=30)
        for tx in txs:
            if tx.in_msg and tx.in_msg.is_internal and tx.in_msg.info.value_coins == pending_deposit.final_amount_nano_ton and tx.now > int((pending_deposit.created_at - timedelta(minutes=5)).timestamp()):
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
                    except: pass
        if pending_deposit.expires_at <= dt.now(timezone.utc) and pending_deposit.status == 'pending': pending_deposit.status = 'expired'; db_session.commit(); return {"status": "expired", "message": "Deposit expired."}
        return {"status": "pending", "message": "Transaction not confirmed."}
    except Exception as e: logger.error(f"Blockchain check error: {e}", exc_info=True); return {"status": "error", "message": "Error checking transaction."}
    finally:
        if provider: await provider.close_all()

@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
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
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth_user_data: return jsonify({"error": "Auth failed"}), 401
    user_id = auth_user_data["id"]; db = next(get_db()); user = db.query(User).filter(User.id == user_id).first()
    if not user: return jsonify({"error": "User not found"}), 404
    if user.referral_earnings_pending > 0:
        withdrawn = user.referral_earnings_pending; user.ton_balance += withdrawn; user.referral_earnings_pending = 0.0; db.commit()
        return jsonify({"status": "success", "message": f"{withdrawn:.2f} TON withdrawn.", "new_balance_ton": user.ton_balance, "new_referral_earnings_pending": 0.0})
    return jsonify({"status": "no_earnings", "message": "No earnings."})

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
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

# Modified endpoint for Tonnel withdrawal
@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST'])
async def withdraw_item_via_tonnel_api(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data:
        return jsonify({"status": "error", "message": "Authentication failed"}), 401
    
    player_user_id = auth_user_data["id"] # This is the RECEIVER of the gift

    if not TONNEL_SENDER_INIT_DATA:
        logger.error("Tonnel withdrawal attempt failed: TONNEL_SENDER_INIT_DATA not configured on backend.")
        return jsonify({"status": "error", "message": "Withdrawal service not configured."}), 500

    db = next(get_db())
    item_to_withdraw = db.query(InventoryItem).filter(
        InventoryItem.id == inventory_item_id,
        InventoryItem.user_id == player_user_id # Ensure the player owns this item
    ).first()

    if not item_to_withdraw:
        return jsonify({"status": "error", "message": "Item not found in your inventory or already withdrawn."}), 404

    item_name_for_tonnel = item_to_withdraw.nft.name
    
    # Initialize TonnelGiftSender with the master sender's authData and secret
    tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret=TONNEL_GIFT_SECRET)
    
    try:
        # The `send_gift_to_user` method is async, so we need to await it.
        # Since Flask routes are sync by default, we use asyncio.run()
        # For production, consider an async framework or task queue for long-running async operations.
        loop = asyncio.get_event_loop()
        if loop.is_closed(): # Create a new event loop if the default one is closed (can happen in some wsgi setups)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        tonnel_result = await tonnel_client.send_gift_to_user(
            gift_item_name=item_name_for_tonnel,
            receiver_telegram_id=player_user_id
        )
        
        if tonnel_result and tonnel_result.get("status") == "success":
            # If Tonnel transfer was successful, then remove from our inventory
            item_value_deducted = item_to_withdraw.current_value # Value to deduct from total_won
            player_profile = db.query(User).filter(User.id == player_user_id).first()
            if player_profile:
                player_profile.total_won_ton = max(0, player_profile.total_won_ton - item_value_deducted)

            db.delete(item_to_withdraw)
            db.commit()
            logger.info(f"Tonnel gift '{item_name_for_tonnel}' successfully sent to user {player_user_id}. Item {inventory_item_id} removed from DB.")
            return jsonify({"status": "success", "message": f"Gift '{item_name_for_tonnel}' sent via Tonnel! {tonnel_result.get('message', '')}", "details": tonnel_result.get("details")})
        else:
            logger.error(f"Tonnel gift sending failed for item '{item_name_for_tonnel}' to user {player_user_id}. Response: {tonnel_result}")
            return jsonify({"status": "error", "message": f"Tonnel withdrawal failed: {tonnel_result.get('message', 'Unknown Tonnel API error')}"}), 500

    except Exception as e:
        logger.error(f"Exception during Tonnel withdrawal process for item {inventory_item_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "An unexpected error occurred during withdrawal."}), 500


# --- Telegram Bot Commands ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"/start from {message.chat.id} ({message.from_user.username}) with text: '{message.text}'")
    db = next(get_db()); user_id = message.chat.id; tg_user_obj = message.from_user
    user = db.query(User).filter(User.id == user_id).first(); created_now = False
    if not user: created_now = True; user = User(id=user_id, username=tg_user_obj.username, first_name=tg_user_obj.first_name, last_name=tg_user_obj.last_name, referral_code=f"ref_{user_id}_{random.randint(1000,9999)}"); db.add(user)
    try:
        command_parts = message.text.split(' ');
        if len(command_parts) > 1 and command_parts[1].startswith('startapp='):
            start_param_value = command_parts[1].split('=')[1]
            if start_param_value.startswith('ref_'):
                referrer_code = start_param_value
                if (created_now or not user.referred_by_id):
                    referrer = db.query(User).filter(User.referral_code == referrer_code).first()
                    if referrer and referrer.id != user.id: user.referred_by_id = referrer.id; logger.info(f"User {user_id} referred by {referrer.id}"); bot.send_message(referrer.id, f"🎉 {user.first_name or user.username or user.id} joined via your link!")
    except Exception as e: logger.error(f"Error processing start param for {user_id}: {e}")
    updated_fields = False
    if user.username != tg_user_obj.username: user.username = tg_user_obj.username; updated_fields = True
    if user.first_name != tg_user_obj.first_name: user.first_name = tg_user_obj.first_name; updated_fields = True
    if user.last_name != tg_user_obj.last_name: user.last_name = tg_user_obj.last_name; updated_fields = True
    if created_now or updated_fields:
        try: db.commit()
        except Exception as e_commit: db.rollback(); logger.error(f"Error saving user {user_id}: {e_commit}")
    
    # Construct Mini App URL for the button
    # MINI_APP_NAME should be your app's short name from BotFather settings for the Mini App
    # Example: if bot is @caseKviBot and Mini App name is "case", URL is https://t.me/caseKviBot/case
    # Ensure MINI_APP_NAME is set in your .env or environment variables
    button_mini_app_url = f"https://t.me/{bot.get_me().username}/{MINI_APP_NAME}"
    if not MINI_APP_NAME:
        logger.error("MINI_APP_NAME is not set. Cannot form correct Mini App button URL.")
        button_mini_app_url = MINI_APP_URL # Fallback to whatever is in MINI_APP_URL if name is missing
        
    markup = types.InlineKeyboardMarkup(); web_app_info = types.WebAppInfo(url=button_mini_app_url)
    app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app_info)
    markup.add(app_button); bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже!", reply_markup=markup)

@bot.message_handler(func=lambda message: True)
def echo_all(message): bot.reply_to(message, "Нажмите /start, чтобы открыть Pusik Gifts.")

# --- Polling ---
bot_polling_started = False; bot_polling_thread = None
def run_bot_polling():
    global bot_polling_started
    if bot_polling_started: return
    bot_polling_started = True; logger.info("Starting bot polling...")
    for i in range(3): try: bot.remove_webhook(); logger.info("Webhook removed."); break; except Exception as e: logger.warning(f"Webhook removal {i+1} failed: {e}"); time.sleep(2)
    while bot_polling_started:
        try: bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code in [401, 409]: bot_polling_started = False; logger.error(f"API error {e.error_code}. Polling stopped."); break
            logger.error(f"Telegram API Exception: {e}", exc_info=True); time.sleep(30)
        except Exception as e: logger.error(f"Critical polling error: {e}", exc_info=True); time.sleep(60)
        if not bot_polling_started: break
        time.sleep(15)
    logger.info("Bot polling loop terminated.")

if __name__ == '__main__':
    if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        bot_polling_thread = threading.Thread(target=run_bot_polling, daemon=True); bot_polling_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
