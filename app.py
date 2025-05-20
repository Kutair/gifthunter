import os
import logging
from flask import Flask, jsonify, request as flask_request
from flask_cors import CORS # Make sure this is imported
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
from decimal import Decimal, ROUND_HALF_UP

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base, backref
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import text

# Imports for Tonnel Withdrawal - Using PyCryptodome
from curl_cffi.requests import AsyncSession, RequestsError
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

# Pytoniq imports
from pytoniq import LiteBalancer
import asyncio

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_NAME = os.environ.get("MINI_APP_NAME", "case")
MINI_APP_URL = os.environ.get("MINI_APP_URL", f"https://t.me/caseKviBot/{MINI_APP_NAME}") # Make sure your bot username is correct here
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24 # 24 hours
TONNEL_SENDER_INIT_DATA = os.environ.get("TONNEL_SENDER_INIT_DATA")
TONNEL_GIFT_SECRET = os.environ.get("TONNEL_GIFT_SECRET", "yowtfisthispieceofshitiiit")

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
if not BOT_TOKEN:
    logger.error("BOT_TOKEN не установлен!")
    exit("BOT_TOKEN is not set. Exiting.")
if not TONNEL_SENDER_INIT_DATA:
    logger.warning("TONNEL_SENDER_INIT_DATA is not set! Tonnel gift withdrawal will likely fail.")

engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Database Models ---
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
    nft_id = Column(Integer, ForeignKey("nfts.id"), nullable=True) 
    item_name_override = Column(String, nullable=True) 
    item_image_override = Column(String, nullable=True)
    current_value = Column(Float, nullable=False) 
    upgrade_multiplier = Column(Float, default=1.0, nullable=False)
    obtained_at = Column(DateTime(timezone=True), server_default=func.now())
    variant = Column(String, nullable=True) 
    is_ton_prize = Column(Boolean, default=False, nullable=False) 
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


# --- Tonnel Gift Sender (AES Encryption part) ---
SALT_SIZE = 8; KEY_SIZE = 32; IV_SIZE = 16
def derive_key_and_iv(passphrase: str, salt: bytes, key_length: int, iv_length: int) -> tuple[bytes, bytes]:
    derived = b''; hasher = hashlib.md5(); hasher.update(passphrase.encode('utf-8')); hasher.update(salt)
    derived_block = hasher.digest(); derived += derived_block
    while len(derived) < key_length + iv_length:
        hasher = hashlib.md5(); hasher.update(derived_block); hasher.update(passphrase.encode('utf-8')); hasher.update(salt)
        derived_block = hasher.digest(); derived += derived_block
    return derived[:key_length], derived[key_length : key_length + iv_length]

def encrypt_aes_cryptojs_compat(plain_text: str, secret_passphrase: str) -> str:
    salt = get_random_bytes(SALT_SIZE); key, iv = derive_key_and_iv(secret_passphrase, salt, KEY_SIZE, IV_SIZE)
    cipher = AES.new(key, AES.MODE_CBC, iv); plain_text_bytes = plain_text.encode('utf-8')
    padded_plain_text = pad(plain_text_bytes, AES.block_size, style='pkcs7'); ciphertext = cipher.encrypt(padded_plain_text)
    salted_ciphertext = b"Salted__" + salt + ciphertext; return base64.b64encode(salted_ciphertext).decode('utf-8')

class TonnelGiftSender: # Same as previous response
    def __init__(self, sender_auth_data: str, gift_secret_passphrase: str):
        self.passphrase_secret = gift_secret_passphrase; self.authdata = sender_auth_data; self._session_instance: AsyncSession | None = None
    async def _get_session(self) -> AsyncSession:
        if self._session_instance is None: self._session_instance = AsyncSession(impersonate="chrome110"); logger.debug("Initialized new AsyncSession for TonnelGiftSender.")
        return self._session_instance
    async def _close_session_if_open(self):
        if self._session_instance: 
            logger.debug("Closing AsyncSession."); 
            try: await self._session_instance.close()
            except Exception as e: logger.error(f"Error closing AsyncSession: {e}")
            finally: self._session_instance = None
    async def _make_request(self, method: str, url: str, headers: dict | None = None, json_payload: dict | None = None, timeout: int = 30, is_initial_get: bool = False):
        session = await self._get_session(); response_obj = None
        try:
            logger.debug(f"Tonnel Req: {method} {url} H: {headers} P: {json_payload}"); kwargs = {"headers": headers, "timeout": timeout}
            if json_payload is not None and method.upper() == "POST": kwargs["json"] = json_payload
            if method.upper() == "GET": response_obj = await session.get(url, **kwargs)
            elif method.upper() == "POST": response_obj = await session.post(url, **kwargs)
            elif method.upper() == "OPTIONS": response_obj = await session.options(url, **kwargs)
            else: raise ValueError(f"Unsupported HTTP method: {method}")
            logger.debug(f"Tonnel Resp: {method} {url} - Status: {response_obj.status_code}, H: {dict(response_obj.headers)}")
            if method.upper() == "OPTIONS":
                if 200 <= response_obj.status_code < 300: return {"status": "options_ok"}
                else: err_txt = await response_obj.text(); logger.error(f"Tonnel OPTIONS {url} fail {response_obj.status_code}. Resp: {err_txt[:500]}"); response_obj.raise_for_status(); return {"status": "error", "message": f"OPTIONS fail: {response_obj.status_code}"}
            response_obj.raise_for_status()
            if response_obj.status_code == 204: return None
            ct = response_obj.headers.get("Content-Type", "").lower()
            if "application/json" in ct:
                try: return response_obj.json()
                except json.JSONDecodeError as je: logger.error(f"Tonnel JSONDecodeError (inner) for {method} {url}: {je}"); err_txt_json = await response_obj.text(); logger.error(f"Resp body for inner JSONDecodeError: {err_txt_json[:500]}"); return {"status": "error", "message": "Invalid JSON in response", "raw_text": err_txt_json[:500]}
            else:
                if is_initial_get: return {"status": "get_ok_non_json"}
                else: resp_txt = await response_obj.text(); logger.warning(f"Tonnel {method} {url} - Resp not JSON. Text: {resp_txt[:200]}"); return {"status": "error", "message": "Response not JSON", "content_type": ct, "text_preview": resp_txt[:200]}
        except RequestsError as re: logger.error(f"Tonnel RequestsError ({method} {url}): {re}"); err_txt_req = ""; raise
        except json.JSONDecodeError as je_outer: logger.error(f"Tonnel JSONDecodeError (outer) {method} {url}: {je_outer}"); raise ValueError(f"Failed to decode JSON from {url}.") from je_outer
        except Exception as e_gen_req: logger.error(f"Tonnel general req error ({method} {url}): {type(e_gen_req).__name__} - {e_gen_req}"); raise
    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int): # Same as previous
        logger.info(f"Attempting Tonnel gift '{gift_item_name}' to user {receiver_telegram_id} ..."); 
        if not self.authdata: logger.error("TONNEL_SENDER_INIT_DATA not configured."); return {"status": "error", "message": "Tonnel sender not configured."}
        try:
            await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)
            filter_str = json.dumps({"price": {"$exists": True}, "refunded": {"$ne": True}, "buyer": {"$exists": False}, "export_at": {"$exists": True}, "gift_name": gift_item_name, "asset": "TON"})
            pg_payload = {"filter": filter_str, "limit": 10, "page": 1, "sort": '{"price":1,"gift_id":-1}'}
            pg_h_opt = {"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type", "Origin": "https://tonnel-gift.vercel.app", "Referer": "https://tonnel-gift.vercel.app/"}
            pg_h_post = {"Content-Type": "application/json", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_h_opt)
            gifts_resp = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_h_post, json_payload=pg_payload)
            if not isinstance(gifts_resp, list) or not gifts_resp: err_msg_g = gifts_resp.get("message", "API error fetching gifts") if isinstance(gifts_resp, dict) else "No gifts found"; logger.error(f"Tonnel: Failed to fetch/find gifts for '{gift_item_name}'. Resp: {gifts_resp}"); return {"status": "error", "message": f"No '{gift_item_name}' gifts or error: {err_msg_g}"}
            low_gift = gifts_resp[0]; logger.info(f"Tonnel: Found gift '{gift_item_name}': ID {low_gift.get('gift_id')}, Price {low_gift.get('price')} TON")
            ui_payload = {"authData": self.authdata, "user": receiver_telegram_id}
            ui_h_common = {"Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            ui_h_opt = {**ui_h_common, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type"}
            ui_h_post = {**ui_h_common, "Content-Type": "application/json"}
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_h_opt)
            user_chk_resp = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_h_post, json_payload=ui_payload)
            if not isinstance(user_chk_resp, dict) or user_chk_resp.get("status") != "success": err_msg_u = user_chk_resp.get("message", "Tonnel rejected user check.") if isinstance(user_chk_resp, dict) else "Unknown user check error."; logger.warning(f"Tonnel: UserInfo check failed. Resp: {user_chk_resp}"); return {"status": "error", "message": f"Tonnel user check failed: {err_msg_u}"}
            ts_str = f"{int(time.time())}"; enc_ts = encrypt_aes_cryptojs_compat(ts_str, self.passphrase_secret)
            buy_url = f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}"
            buy_payload = {"anonymously": True, "asset": "TON", "authData": self.authdata, "price": low_gift['price'], "receiver": receiver_telegram_id, "showPrice": False, "timestamp": enc_ts}
            buy_h_common = {"Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/", "Host": "gifts.coffin.meme"}
            buy_h_opt = {**buy_h_common, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type"}
            buy_h_post = {**buy_h_common, "Content-Type": "application/json"}
            await self._make_request(method="OPTIONS", url=buy_url, headers=buy_h_opt)
            purch_resp = await self._make_request(method="POST", url=buy_url, headers=buy_h_post, json_payload=buy_payload, timeout=90)
            logger.info(f"Tonnel: BuyGift response: {purch_resp}")
            if isinstance(purch_resp, dict) and purch_resp.get("status") == "success": return {"status": "success", "message": f"Gift '{gift_item_name}' sent!", "details": purch_resp}
            else: err_msg_b = purch_resp.get("message", "Tonnel rejected purchase.") if isinstance(purch_resp, dict) else "Unknown purchase error."; logger.error(f"Tonnel: Failed to send gift. Resp: {purch_resp}"); return {"status": "error", "message": f"Tonnel transfer failed: {err_msg_b}"}
        except ValueError as ve_send: logger.error(f"Tonnel ValueError: {ve_send}", exc_info=True); return {"status": "error", "message": f"Tonnel API error: {str(ve_send)}"}
        except RequestsError as re_send: logger.error(f"Tonnel RequestsError: {re_send}", exc_info=True); return {"status": "error", "message": f"Tonnel network error: {str(re_send)}"}
        except Exception as e_send: logger.error(f"Tonnel Unexpected error: {type(e_send).__name__} - {e_send}", exc_info=True); return {"status": "error", "message": f"Unexpected error: {str(e_send)}"}
        finally: await self._close_session_if_open()

# --- Game Data & Initial Setup (Same as previous response) ---
def generate_image_filename_from_name(name_str: str) -> str: # Same
    if not name_str: return 'placeholder.png'
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png"
    name_str_rep = name_str.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'
    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    return re.sub(r'-+', '-', cleaned) + '.png'
UPDATED_FLOOR_PRICES = { 'Plush Pepe': 1200.0, 'Neko Helmet': 15.0, 'Sharp Tongue': 17.0, "Durov's Cap": 251.0, 'Voodoo Doll': 9.4, 'Vintage Cigar': 19.7, 'Astral Shard': 50.0, 'Scared Cat': 22.0, 'Swiss Watch': 18.6, 'Perfume Bottle': 38.3, 'Precious Peach': 100.0, 'Toy Bear': 16.3, 'Genie Lamp': 19.3, 'Loot Bag': 25.0, 'Kissed Frog': 14.8, 'Electric Skull': 10.9, 'Diamond Ring': 8.06, 'Mini Oscar': 40.5, 'Party Sparkler': 2.0, 'Homemade Cake': 2.0, 'Cookie Heart': 1.8, 'Jack-in-the-box': 2.0, 'Skull Flower': 3.4, 'Lol Pop': 1.4, 'Hynpo Lollipop': 1.4, 'Desk Calendar': 1.4, 'B-Day Candle': 1.4, 'Record Player': 4.0, 'Jelly Bunny': 3.6, 'Tama Gadget': 4.0, 'Snow Globe': 4.0, 'Eternal Rose': 11.0, 'Love Potion': 5.4, 'Top Hat': 6.0 }
TON_PRIZE_IMAGE_DEFAULT = generate_image_filename_from_name(None)
cases_data_backend_with_fixed_prices = [ # Same structure
    { 'id': 'lolpop', 'name': 'Lol Pop Stash', 'priceTON': 1.5, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.001 }, { 'name': 'Neko Helmet', 'probability': 0.005 }, { 'name': 'Party Sparkler', 'probability': 0.07 }, { 'name': 'Homemade Cake', 'probability': 0.07 }, { 'name': 'Cookie Heart', 'probability': 0.07 }, { 'name': 'Jack-in-the-box', 'probability': 0.06 }, { 'name': 'Skull Flower', 'probability': 0.023 }, { 'name': 'Lol Pop', 'probability': 0.25 }, { 'name': 'Hynpo Lollipop', 'probability': 0.25 }, { 'name': 'Desk Calendar', 'probability': 0.10 }, { 'name': 'B-Day Candle', 'probability': 0.101 } ] },
    { 'id': 'recordplayer', 'name': 'Record Player Vault', 'priceTON': 6.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0012 }, { 'name': 'Record Player', 'probability': 0.40 }, { 'name': 'Lol Pop', 'probability': 0.10 }, { 'name': 'Hynpo Lollipop', 'probability': 0.10 }, { 'name': 'Party Sparkler', 'probability': 0.10 }, { 'name': 'Skull Flower', 'probability': 0.10 }, { 'name': 'Jelly Bunny', 'probability': 0.0988 }, { 'name': 'Tama Gadget', 'probability': 0.05 }, { 'name': 'Snow Globe', 'probability': 0.05 } ] },
    { 'id': 'swisswatch', 'name': 'Swiss Watch Box', 'priceTON': 10.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0015 }, { 'name': 'Swiss Watch', 'probability': 0.08 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Eternal Rose', 'probability': 0.05 }, { 'name': 'Electric Skull', 'probability': 0.03 }, { 'name': 'Diamond Ring', 'probability': 0.0395 }, { 'name': 'Record Player', 'probability': 0.20 }, { 'name': 'Love Potion', 'probability': 0.20 }, { 'name': 'Top Hat', 'probability': 0.15 }, { 'name': 'Voodoo Doll', 'probability': 0.149 } ] },
    { 'id': 'perfumebottle', 'name': 'Perfume Chest', 'priceTON': 20.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0018 }, { 'name': 'Perfume Bottle', 'probability': 0.08 }, { 'name': 'Sharp Tongue', 'probability': 0.12 }, { 'name': 'Loot Bag', 'probability': 0.09946 }, { 'name': 'Swiss Watch', 'probability': 0.15 }, { 'name': 'Neko Helmet', 'probability': 0.15 }, { 'name': 'Genie Lamp', 'probability': 0.15 }, { 'name': 'Kissed Frog', 'probability': 0.10 }, { 'name': 'Electric Skull', 'probability': 0.07 }, { 'name': 'Diamond Ring', 'probability': 0.07874 } ] },
    { 'id': 'vintagecigar', 'name': 'Vintage Cigar Safe', 'priceTON': 40.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.002 }, { 'name': 'Perfume Bottle', 'probability': 0.2994 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.12 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Sharp Tongue', 'probability': 0.10 }, { 'name': 'Genie Lamp', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.08 }, { 'name': 'Scared Cat', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.0486 } ] },
    { 'id': 'astralshard', 'name': 'Astral Shard Relic', 'priceTON': 100.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0025 }, { 'name': 'Durov\'s Cap', 'probability': 0.09925 }, { 'name': 'Astral Shard', 'probability': 0.10 }, { 'name': 'Precious Peach', 'probability': 0.10 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Perfume Bottle', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.10 }, { 'name': 'Neko Helmet', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.10 }, { 'name': 'Scared Cat', 'probability': 0.08 }, { 'name': 'Loot Bag', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.04825 } ] },
    { 'id': 'plushpepe', 'name': 'Plush Pepe Hoard', 'priceTON': 200.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.15 }, { 'name': 'Durov\'s Cap', 'probability': 0.25 }, { 'name': 'Astral Shard', 'probability': 0.60 } ] },
    { 'id': 'black', 'name': 'BLACK Singularity', 'isBackgroundCase': True, 'bgImageFilename': 'image-1.png', 'overlayPrizeName': 'Neko Helmet', 'priceTON': 30.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.001 }, { 'name': 'Durov\'s Cap', 'probability': 0.01 }, { 'name': 'Perfume Bottle', 'probability': 0.05 }, { 'name': 'Mini Oscar', 'probability': 0.04 }, { 'name': 'Scared Cat', 'probability': 0.06 }, { 'name': 'Vintage Cigar', 'probability': 0.07 }, { 'name': 'Loot Bag', 'probability': 0.07 }, { 'name': 'Sharp Tongue', 'probability': 0.08 }, { 'name': 'Genie Lamp', 'probability': 0.08 }, { 'name': 'Swiss Watch', 'probability': 0.10 }, { 'name': 'Neko Helmet', 'probability': 0.15 }, { 'name': 'Kissed Frog', 'probability': 0.10 }, { 'name': 'Electric Skull', 'probability': 0.09 }, { 'name': 'Diamond Ring', 'probability': 0.089} ] }
]
cases_data_backend = []
for case_template in cases_data_backend_with_fixed_prices: # Same
    processed_case = {**case_template}
    if not processed_case.get('isBackgroundCase'): processed_case['imageFilename'] = generate_image_filename_from_name(processed_case['name'])
    full_prizes = []
    for prize_stub in processed_case['prizes']: prize_name = prize_stub['name']; full_prizes.append({'name': prize_name, 'imageFilename': generate_image_filename_from_name(prize_name), 'floorPrice': UPDATED_FLOOR_PRICES.get(prize_name, 0), 'probability': prize_stub['probability']})
    processed_case['prizes'] = full_prizes; cases_data_backend.append(processed_case)
DEFAULT_SLOT_TON_PRIZES = [ {'name': "0.1 TON", 'value': 0.1, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.25 TON", 'value': 0.25, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.5 TON", 'value': 0.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.0 TON", 'value': 1.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.5 TON", 'value': 1.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
PREMIUM_SLOT_TON_PRIZES = [ {'name': "2 TON", 'value': 2.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "3 TON", 'value': 3.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "5 TON", 'value': 5.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "10 TON", 'value': 10.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
ALL_ITEMS_POOL_FOR_SLOTS = [{'name': name, 'floorPrice': price, 'imageFilename': generate_image_filename_from_name(name), 'is_ton_prize': False} for name, price in UPDATED_FLOOR_PRICES.items()]
slots_data_backend = [ { 'id': 'default_slot', 'name': 'Default Slot', 'priceTON': 3.0, 'reels_config': 3, 'prize_pool': [] }, { 'id': 'premium_slot', 'name': 'Premium Slot', 'priceTON': 10.0, 'reels_config': 3, 'prize_pool': [] } ]
def finalize_slot_prize_pools(): # Same
    global slots_data_backend
    for slot_data in slots_data_backend:
        temp_pool = []
        if slot_data['id'] == 'default_slot':
            prob_ton = (0.50 / len(DEFAULT_SLOT_TON_PRIZES)) if DEFAULT_SLOT_TON_PRIZES else 0; [temp_pool.append({**p, 'probability': prob_ton}) for p in DEFAULT_SLOT_TON_PRIZES]
            items = [i for i in ALL_ITEMS_POOL_FOR_SLOTS if i['floorPrice'] < 15]; items = items if items else ALL_ITEMS_POOL_FOR_SLOTS[:10] if ALL_ITEMS_POOL_FOR_SLOTS else []
            if items: prob_item = 0.50 / len(items); [temp_pool.append({**i, 'probability': prob_item}) for i in items]
        elif slot_data['id'] == 'premium_slot':
            prob_ton = (0.40 / len(PREMIUM_SLOT_TON_PRIZES)) if PREMIUM_SLOT_TON_PRIZES else 0; [temp_pool.append({**p, 'probability': prob_ton}) for p in PREMIUM_SLOT_TON_PRIZES]
            items = [i for i in ALL_ITEMS_POOL_FOR_SLOTS if i['floorPrice'] >= 15]; items = items if items else ALL_ITEMS_POOL_FOR_SLOTS[-10:] if ALL_ITEMS_POOL_FOR_SLOTS else []
            if items: prob_item = 0.60 / len(items); [temp_pool.append({**i, 'probability': prob_item}) for i in items]
        total_prob = sum(p.get('probability', 0) for p in temp_pool)
        if total_prob > 0 and abs(total_prob - 1.0) > 0.001: logger.warning(f"Normalizing slot {slot_data['id']}. Sum: {total_prob}"); [p.update({'probability': p.get('probability',0)/total_prob}) for p in temp_pool]
        slot_data['prize_pool'] = temp_pool
finalize_slot_prize_pools()
def populate_initial_data(): # Same
    db = SessionLocal(); 
    try:
        for name, price in UPDATED_FLOOR_PRICES.items():
            nft = db.query(NFT).filter(NFT.name == name).first()
            if not nft: db.add(NFT(name=name, image_filename=generate_image_filename_from_name(name), floor_price=price))
            elif nft.floor_price != price or nft.image_filename != generate_image_filename_from_name(name): nft.floor_price = price; nft.image_filename = generate_image_filename_from_name(name)
        db.commit(); logger.info("Initial NFT data populated/updated.")
    except Exception as e_pop: db.rollback(); logger.error(f"Error populating NFT data: {e_pop}")
    finally: db.close()
def initial_setup_and_logging(): populate_initial_data() # Add RTP log if needed
initial_setup_and_logging()

# --- Flask App & Routes (Largely same as previous, minor logging adjustments) ---
DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR" 
DEPOSIT_COMMENT = "cpd7r07ud3s"; PENDING_DEPOSIT_EXPIRY_MINUTES = 30
app = Flask(__name__)
PROD_ORIGIN = "https://vasiliy-katsyka.github.io"; NULL_ORIGIN = "null"; LOCAL_DEV_ORIGINS = ["http://localhost:5500", "http://127.0.0.1:5500", "http://localhost:8000", "http://127.0.0.1:8000"]
final_allowed_origins = list(set([PROD_ORIGIN, NULL_ORIGIN] + LOCAL_DEV_ORIGINS))
CORS(app, resources={r"/api/*": {"origins": final_allowed_origins}})
bot = telebot.TeleBot(BOT_TOKEN)
def get_db(): db = SessionLocal(); try: yield db; finally: db.close()
def validate_init_data(init_data_str: str, bot_token: str) -> dict | None: # Same, with improved logging
    if not init_data_str: logger.warning("validate_init_data: init_data_str is empty."); return None
    try:
        logger.debug(f"Validating init_data (first 100): {init_data_str[:100]}"); parsed = dict(parse_qs(init_data_str))
        if not all(k in parsed for k in ['hash', 'user', 'auth_date']): logger.warning(f"validate_init_data: Missing keys. Has: {list(parsed.keys())}"); return None
        rcv_hash = parsed.pop('hash')[0]; auth_ts = int(parsed['auth_date'][0]); cur_ts = int(dt.now(timezone.utc).timestamp())
        if (cur_ts - auth_ts) > AUTH_DATE_MAX_AGE_SECONDS: logger.warning(f"validate_init_data: Expired. Age: {cur_ts - auth_ts}s"); return None
        chk_str_parts = [f"{k}={parsed[k][0]}" for k in sorted(parsed.keys())]; chk_str = "\n".join(chk_str_parts)
        secret = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calc_hash = hmac.new(secret, chk_str.encode(), hashlib.sha256).hexdigest()
        if calc_hash == rcv_hash:
            user_info = json.loads(unquote(parsed['user'][0])); 
            if 'id' not in user_info: logger.warning("validate_init_data: 'id' missing in user_info."); return None
            user_info['id'] = int(user_info['id']); logger.info(f"validate_init_data: OK for user: {user_info.get('id')}"); return user_info
        else: logger.warning("validate_init_data: Hash mismatch."); return None
    except Exception as e_val: logger.error(f"validate_init_data: Error: {e_val}", exc_info=True); return None
@app.route('/')
def index_route(): return "Pusik Gifts App is Running!"
@app.route('/api/get_user_data', methods=['POST']) # Same, with robust inventory serialization
def get_user_data_api():
    init_data = flask_request.headers.get('X-Telegram-Init-Data'); logger.info(f"/api/get_user_data. InitData Header: {'Yes' if init_data else 'No'}")
    auth = validate_init_data(init_data, BOT_TOKEN)
    if not auth: logger.warning("/api/get_user_data: Auth failed."); return jsonify({"error": "Auth failed."}), 401
    uid = auth["id"]; logger.info(f"/api/get_user_data: Auth user ID: {uid}"); db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            logger.info(f"/api/get_user_data: User {uid} not found, creating."); user = User(id=uid, username=auth.get("username"), first_name=auth.get("first_name"), last_name=auth.get("last_name"))
            if not user.referral_code: user.referral_code = f"ref_{uid}_{random.randint(1000,9999)}"; db.add(user); db.commit(); db.refresh(user); logger.info(f"/api/get_user_data: New user {uid} created.")
        else: # Update existing user info from TG if changed
            logger.info(f"/api/get_user_data: User {uid} found."); changed = False
            if user.username != auth.get("username"): user.username = auth.get("username"); changed = True
            if user.first_name != auth.get("first_name"): user.first_name = auth.get("first_name"); changed = True
            if user.last_name != auth.get("last_name"): user.last_name = auth.get("last_name"); changed = True
            if changed: db.commit(); logger.info(f"User {uid} info updated from TG.")
        inv_list = []
        for i_item in user.inventory:
            name = i_item.nft.name if i_item.nft else i_item.item_name_override or "Item"
            img_file = i_item.nft.image_filename if i_item.nft else i_item.item_image_override or generate_image_filename_from_name(None)
            f_price = float(i_item.nft.floor_price) if i_item.nft else float(i_item.current_value)
            inv_list.append({"id": i_item.id, "name": name, "imageFilename": img_file, "floorPrice": f_price, "currentValue": float(i_item.current_value), "upgradeMultiplier": float(i_item.upgrade_multiplier), "variant": i_item.variant, "is_ton_prize": i_item.is_ton_prize, "obtained_at": i_item.obtained_at.isoformat() if i_item.obtained_at else None})
        ref_count = db.query(User).filter(User.referred_by_id == uid).count()
        resp_data = {"id": user.id, "username": user.username, "first_name": user.first_name, "last_name": user.last_name, "tonBalance": float(user.ton_balance), "starBalance": int(user.star_balance), "inventory": inv_list, "referralCode": user.referral_code, "referralEarningsPending": float(user.referral_earnings_pending), "total_won_ton": float(user.total_won_ton), "invited_friends_count": ref_count}
        logger.info(f"/api/get_user_data: Resp for {uid} (no inv): { {k:v for k,v in resp_data.items() if k != 'inventory'} }")
        return jsonify(resp_data)
    except Exception as e_get_user: db.rollback(); logger.error(f"/api/get_user_data error for {uid if 'uid' in locals() else 'unknown'}: {e_get_user}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

# --- Other API Routes (open_case, spin_slot, upgrade_item, etc. - Same as previous response) ---
# Ensure that the logic for these routes is complete and correct as per the previous iteration.
# For brevity, I am not re-pasting all of them here if they were correct.
# The most important part was the user data fetching.

# Pasting the rest of the routes to be complete
@app.route('/api/open_case', methods=['POST']) # Same logic as before
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); cid = data.get('case_id'); multiplier = int(data.get('multiplier', 1))
    if not cid or multiplier not in [1,2,3]: return jsonify({"error": "Invalid params"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        tcase = next((c for c in cases_data_backend if c['id'] == cid), None)
        if not tcase: return jsonify({"error": "Case not found"}), 404
        base_cost = Decimal(str(tcase['priceTON'])); total_cost = base_cost * Decimal(multiplier)
        if Decimal(str(user.ton_balance)) < total_cost: return jsonify({"error": f"Not enough TON. Need {total_cost:.2f}"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - total_cost)
        prizes_in_case = tcase['prizes']; won_prizes_list = []; total_value_this_spin = Decimal('0')
        for _ in range(multiplier):
            rv = random.random(); cprob = 0; chosen_prize_info = None
            for p_info in prizes_in_case:
                cprob += p_info['probability']
                if rv <= cprob: chosen_prize_info = p_info; break
            if not chosen_prize_info: chosen_prize_info = random.choice(prizes_in_case) if prizes_in_case else None
            if not chosen_prize_info: continue 
            dbnft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
            if not dbnft: logger.error(f"NFT {chosen_prize_info['name']} missing!"); continue
            variant = "black_singularity" if tcase.get('id') == 'black' else None
            actual_val = Decimal(str(dbnft.floor_price)) * (Decimal('2.5') if variant == "black_singularity" else Decimal('1'))
            total_value_this_spin += actual_val
            item = InventoryItem(user_id=uid, nft_id=dbnft.id, current_value=float(actual_val.quantize(Decimal('0.01'))), variant=variant)
            db.add(item); db.flush() 
            won_prizes_list.append({"id": item.id, "name": dbnft.name, "imageFilename": dbnft.image_filename, "floorPrice": float(dbnft.floor_price), "currentValue": item.current_value, "variant": item.variant, "is_ton_prize": False }) # Cases give items, not direct TON
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status": "success", "won_prizes": won_prizes_list, "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Open case error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/spin_slot', methods=['POST']) # Same logic
def spin_slot_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); slot_id = data.get('slot_id')
    if not slot_id: return jsonify({"error": "slot_id required"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        target_slot = next((s for s in slots_data_backend if s['id'] == slot_id), None)
        if not target_slot: return jsonify({"error": "Slot not found"}), 404
        cost = Decimal(str(target_slot['priceTON']))
        if Decimal(str(user.ton_balance)) < cost: return jsonify({"error": f"Not enough TON. Need {cost:.2f}"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - cost)
        num_reels = target_slot.get('reels_config', 3); slot_pool = target_slot['prize_pool']
        if not slot_pool: return jsonify({"error": "Slot prize pool empty"}), 500
        reel_results_data = [] 
        for _ in range(num_reels):
            rv = random.random(); cprob = 0; landed = None
            for p_info_slot in slot_pool: cprob += p_info_slot.get('probability', 0); 
                if rv <= cprob: landed = p_info_slot; break
            if not landed: landed = random.choice(slot_pool) if slot_pool else None
            if landed: reel_results_data.append(landed)
            else: reel_results_data.append({"name": "Error", "imageFilename": generate_image_filename_from_name(None), "is_ton_prize": False, "value": 0})
        won_prizes_from_slot = []; total_value_this_spin = Decimal('0')
        for landed_item_data in reel_results_data:
            if landed_item_data.get('is_ton_prize'):
                ton_val = Decimal(str(landed_item_data['value']))
                won_prizes_from_slot.append({"id": f"ton_{int(time.time()*1e3)}_{random.randint(0,999)}", "name": landed_item_data['name'], "imageFilename": landed_item_data.get('imageFilename', TON_PRIZE_IMAGE_DEFAULT), "currentValue": float(ton_val), "is_ton_prize": True})
                total_value_this_spin += ton_val; user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_val)
        if num_reels > 0 and all(not r.get('is_ton_prize') for r in reel_results_data):
            first_item_name = reel_results_data[0]['name']
            if all(r['name'] == first_item_name for r in reel_results_data):
                won_item_data = reel_results_data[0] 
                db_nft = db.query(NFT).filter(NFT.name == won_item_data['name']).first()
                if db_nft:
                    actual_val = Decimal(str(db_nft.floor_price)); inv_item = InventoryItem(user_id=uid, nft_id=db_nft.id, current_value=float(actual_val), variant=None)
                    db.add(inv_item); db.flush()
                    won_prizes_from_slot.append({"id": inv_item.id, "name": db_nft.name, "imageFilename": db_nft.image_filename, "floorPrice": float(db_nft.floor_price), "currentValue": inv_item.current_value, "is_ton_prize": False, "variant": inv_item.variant})
                    total_value_this_spin += actual_val
                else: logger.error(f"Slot item win: NFT '{won_item_data['name']}' not in DB.")
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin); db.commit()
        return jsonify({"status": "success", "reel_results": reel_results_data, "won_prizes": won_prizes_from_slot, "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Spin slot error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/upgrade_item', methods=['POST']) # Same
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id'); mult_str = data.get('multiplier_str')
    if not all([iid, mult_str]): return jsonify({"error": "Missing params"}), 400
    try: mult = Decimal(mult_str); iid_int = int(iid)
    except: return jsonify({"error": "Invalid data"}), 400
    chances = {Decimal("1.5"):50, Decimal("2.0"):35, Decimal("3.0"):25, Decimal("5.0"):15, Decimal("10.0"):8, Decimal("20.0"):3}
    if mult not in chances: return jsonify({"error": "Invalid multiplier"}), 400
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_int, InventoryItem.user_id == uid).with_for_update().first()
        if not item or item.is_ton_prize: return jsonify({"error": "Item not found or not upgradable"}), 404
        user = db.query(User).filter(User.id == uid).with_for_update().first() 
        if random.uniform(0,100) < chances[mult]:
            orig_val = Decimal(str(item.current_value)); new_val = (orig_val * mult).quantize(Decimal('0.01'), ROUND_HALF_UP)
            increase = new_val - orig_val; item.current_value = float(new_val); item.upgrade_multiplier = float(Decimal(str(item.upgrade_multiplier)) * mult)
            if user: user.total_won_ton = float(Decimal(str(user.total_won_ton)) + increase)
            db.commit(); nft_name = item.nft.name if item.nft else item.item_name_override
            return jsonify({"status": "success", "message": f"Upgraded! New value: {new_val:.2f} TON", "item": {"id": item.id, "name": nft_name, "currentValue": item.current_value, "upgradeMultiplier": item.upgrade_multiplier, "variant": item.variant }})
        else:
            name_lost = item.nft.name if item.nft else item.item_name_override; val_lost = Decimal(str(item.current_value))
            if user: user.total_won_ton = float(Decimal(str(user.total_won_ton)) - val_lost)
            db.delete(item); db.commit(); return jsonify({"status": "failed", "message": f"Upgrade failed! Lost {name_lost}.", "item_lost": True})
    except Exception as e: db.rollback(); logger.error(f"Upgrade error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/convert_to_ton', methods=['POST']) # Same
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id')
    if not iid: return jsonify({"error": "ID required"}), 400
    try: iid_int = int(iid)
    except ValueError: return jsonify({"error": "Invalid ID"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_int, InventoryItem.user_id == uid).with_for_update().first()
        if not user or not item: return jsonify({"error": "User or item not found"}), 404
        if item.is_ton_prize: return jsonify({"error": "Cannot convert TON prize."}), 400
        val = Decimal(str(item.current_value)); user.ton_balance = float(Decimal(str(user.ton_balance)) + val)
        item_name = item.nft.name if item.nft else item.item_name_override; db.delete(item); db.commit()
        return jsonify({"status": "success", "message": f"{item_name} sold for {val:.2f} TON.", "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Convert error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/sell_all_items', methods=['POST']) # Same
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        items_to_sell = [i for i in user.inventory if not i.is_ton_prize]
        if not items_to_sell: return jsonify({"status": "no_items", "message": "No sellable items."})
        total_val = sum(Decimal(str(i.current_value)) for i in items_to_sell); user.ton_balance = float(Decimal(str(user.ton_balance)) + total_val)
        for i_del in items_to_sell: db.delete(i_del)
        db.commit(); return jsonify({"status": "success", "message": f"All {len(items_to_sell)} sellable items sold for {total_val:.2f} TON.", "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Sell all error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/initiate_deposit', methods=['POST']) # Same
def initiate_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); amt_str = data.get('amount')
    if amt_str is None: return jsonify({"error": "Amount required"}), 400
    try: orig_amt = float(amt_str)
    except ValueError: return jsonify({"error": "Invalid amount"}), 400
    if not (0.1 <= orig_amt <= 10000): return jsonify({"error": "Amount out of range (0.1 to 10000 TON)"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user: return jsonify({"error": "User not found"}), 404
        if db.query(PendingDeposit).filter(PendingDeposit.user_id == uid, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first(): return jsonify({"error": "Active deposit exists."}), 409
        nano_part = random.randint(10000, 999999); final_nano_amt = int(orig_amt * 1e9) + nano_part
        pdep = PendingDeposit(user_id=uid, original_amount_ton=orig_amt, unique_identifier_nano_ton=nano_part, final_amount_nano_ton=final_nano_amt, expected_comment=DEPOSIT_COMMENT, expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
        db.add(pdep); db.commit(); db.refresh(pdep); disp_amt = f"{final_nano_amt / 1e9:.9f}".rstrip('0').rstrip('.')
        return jsonify({"status": "success", "pending_deposit_id": pdep.id, "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW, "amount_to_send": disp_amt, "final_amount_nano_ton": final_nano_amt, "comment": DEPOSIT_COMMENT, "expires_at": pdep.expires_at.isoformat()})
    except Exception as e: db.rollback(); logger.error(f"Init deposit error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

async def check_blockchain_for_deposit(pdep: PendingDeposit, db_sess): # Same
    prov = None
    try:
        prov = LiteBalancer.from_mainnet_config(trust_level=2); await prov.start_up()
        txs = await prov.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=30)
        for tx in txs:
            if tx.in_msg and tx.in_msg.is_internal and tx.in_msg.info.value_coins == pdep.final_amount_nano_ton and tx.now > int((pdep.created_at - timedelta(minutes=5)).timestamp()):
                cmt_slice = tx.in_msg.body.begin_parse()
                if cmt_slice.remaining_bits >= 32 and cmt_slice.load_uint(32) == 0: 
                    try:
                        comment_text = cmt_slice.load_snake_string()
                        if comment_text == pdep.expected_comment:
                            usr = db_sess.query(User).filter(User.id == pdep.user_id).with_for_update().first()
                            if not usr: pdep.status = 'failed'; db_sess.commit(); return {"status": "error", "message": "User not found."}
                            usr.ton_balance = float(Decimal(str(usr.ton_balance)) + Decimal(str(pdep.original_amount_ton)))
                            if usr.referred_by_id:
                                referrer = db_sess.query(User).filter(User.id == usr.referred_by_id).with_for_update().first()
                                if referrer: ref_bonus = (Decimal(str(pdep.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP); referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + ref_bonus)
                            pdep.status = 'completed'; db_sess.commit()
                            return {"status": "success", "message": "Deposit confirmed!", "new_balance_ton": usr.ton_balance}
                    except Exception as e_cmt: logger.debug(f"Comment parse issue for tx {tx.hash}: {e_cmt}"); pass
        if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending': pdep.status = 'expired'; db_sess.commit(); return {"status": "expired", "message": "Deposit expired."}
        return {"status": "pending", "message": "Transaction not confirmed yet."}
    except Exception as e_bc: logger.error(f"Blockchain check error: {e_bc}", exc_info=True); return {"status": "error", "message": "Blockchain check error."}
    finally:
        if prov: await prov.close_all()

@app.route('/api/verify_deposit', methods=['POST']) # Same
def verify_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); pid = data.get('pending_deposit_id')
    if not pid: return jsonify({"error": "ID required"}), 400
    db = next(get_db())
    try:
        pdep = db.query(PendingDeposit).filter(PendingDeposit.id == pid, PendingDeposit.user_id == uid).with_for_update().first()
        if not pdep: return jsonify({"error": "Deposit not found"}), 404
        if pdep.status == 'completed': usr = db.query(User).filter(User.id == uid).first(); return jsonify({"status": "success", "message": "Already confirmed.", "new_balance_ton": usr.ton_balance if usr else 0})
        if pdep.status == 'expired' or pdep.expires_at <= dt.now(timezone.utc):
            if pdep.status == 'pending': pdep.status = 'expired'; db.commit()
            return jsonify({"status": "expired", "message": "Deposit expired."}), 400
        result = {}
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running(): new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop); result = new_loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
            else: result = loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
        except RuntimeError as e_rt: logger.info(f"RuntimeError for asyncio in verify_deposit, new loop: {e_rt}"); new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop); result = new_loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
        return jsonify(result)
    except Exception as e: db.rollback(); logger.error(f"Verify deposit error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/get_leaderboard', methods=['GET']) # Same
def get_leaderboard_api():
    db = next(get_db()); 
    try: leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all(); return jsonify([{"rank": r+1, "name": u.first_name or u.username or f"User_{str(u.id)[:6]}", "avatarChar": (u.first_name or u.username or "U")[0].upper(), "income": u.total_won_ton, "user_id": u.id} for r, u in enumerate(leaders)])
    finally: db.close()

@app.route('/api/withdraw_referral_earnings', methods=['POST']) # Same
def withdraw_referral_earnings_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        if user.referral_earnings_pending > 0:
            withdrawn = Decimal(str(user.referral_earnings_pending)); user.ton_balance = float(Decimal(str(user.ton_balance)) + withdrawn); user.referral_earnings_pending = 0.0; db.commit()
            return jsonify({"status": "success", "message": f"{withdrawn:.2f} TON withdrawn.", "new_balance_ton": user.ton_balance, "new_referral_earnings_pending": 0.0})
        else: return jsonify({"status": "no_earnings", "message": "No earnings."})
    except Exception as e: db.rollback(); logger.error(f"Withdraw ref error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/redeem_promocode', methods=['POST']) # Same
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); code_txt = data.get('promocode_text', "").strip()
    if not code_txt: return jsonify({"status": "error", "message": "Code empty."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first(); promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).with_for_update().first()
        if not user: return jsonify({"status": "error", "message": "User not found."}), 404
        if not promo: return jsonify({"status": "error", "message": "Invalid code."}), 404
        if promo.activations_left <= 0: return jsonify({"status": "error", "message": "Code expired."}), 400
        promo.activations_left -= 1; user.ton_balance = float(Decimal(str(user.ton_balance)) + Decimal(str(promo.ton_amount))); db.commit()
        return jsonify({"status": "success", "message": f"Redeemed! +{promo.ton_amount:.2f} TON.", "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Promo error: {e}", exc_info=True); return jsonify({"status": "error", "message": "DB error."}), 500
    finally: db.close()

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST']) # Same
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id):
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"status": "error", "message": "Auth failed"}), 401
    player_id = auth["id"]; 
    if not TONNEL_SENDER_INIT_DATA: return jsonify({"status": "error", "message": "Withdrawal unavailable."}), 500
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == player_id).with_for_update().first()
        if not item or item.is_ton_prize: return jsonify({"status": "error", "message": "Item not found/withdrawable."}), 404
        item_name_tonnel = item.nft.name if item.nft else item.item_name_override
        client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET); result = {}
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running(): new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop); result = new_loop.run_until_complete(client.send_gift_to_user(gift_item_name=item_name_tonnel, receiver_telegram_id=player_id))
            else: result = loop.run_until_complete(client.send_gift_to_user(gift_item_name=item_name_tonnel, receiver_telegram_id=player_id))
        except RuntimeError as e_rt_tonnel: logger.info(f"RuntimeError for asyncio in Tonnel withdraw, new loop: {e_rt_tonnel}"); new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop); result = new_loop.run_until_complete(client.send_gift_to_user(gift_item_name=item_name_tonnel, receiver_telegram_id=player_id))
        if result and result.get("status") == "success":
            val_deducted = Decimal(str(item.current_value)); player_user = db.query(User).filter(User.id == player_id).with_for_update().first()
            if player_user: player_user.total_won_ton = float(max(Decimal('0'), Decimal(str(player_user.total_won_ton)) - val_deducted))
            db.delete(item); db.commit(); return jsonify({"status": "success", "message": f"Gift '{item_name_tonnel}' sent! {result.get('message', '')}", "details": result.get("details")})
        else: db.rollback(); return jsonify({"status": "error", "message": f"Tonnel failed: {result.get('message', 'API error')}"}), 500
    except Exception as e: db.rollback(); logger.error(f"Tonnel withdraw error: {e}", exc_info=True); return jsonify({"status": "error", "message": "Unexpected error."}), 500
    finally: db.close()

# --- Telegram Bot Handlers (Same as previous response) ---
@bot.message_handler(commands=['start'])
def send_welcome(message): # Same logic
    logger.info(f"/start from {message.chat.id} ({message.from_user.username}) text: '{message.text}'"); db = next(get_db())
    try:
        uid = message.chat.id; tg_user = message.from_user; user = db.query(User).filter(User.id == uid).first(); is_new = not user
        if is_new: user = User(id=uid, username=tg_user.username, first_name=tg_user.first_name, last_name=tg_user.last_name, referral_code=f"ref_{uid}_{random.randint(1000,9999)}"); db.add(user)
        try:
            parts = message.text.split(' '); 
            if len(parts) > 1 and parts[1].startswith('startapp='):
                param = parts[1].split('=', 1)[1]
                if param.startswith('ref_') and (is_new or not user.referred_by_id) and user.referral_code != param:
                    referrer = db.query(User).filter(User.referral_code == param, User.id != user.id).first()
                    if referrer: user.referred_by_id = referrer.id; logger.info(f"User {uid} referred by {referrer.id} via {param}.");
                        try: bot.send_message(referrer.id, f"🎉 Friend {user.first_name or user.username or user.id} joined via your link!")
                        except Exception as e_notify_ref: logger.warning(f"Failed to notify referrer {referrer.id}: {e_notify_ref}")
                    else: logger.warning(f"Referral code {param} not found or self-ref by {uid}.")
        except Exception as e_ref_link_proc: logger.error(f"Error processing deep link for {uid}: {e_ref_link_proc}")
        updated_info = False
        if user.username != tg_user.username: user.username = tg_user.username; updated_info = True
        if user.first_name != tg_user.first_name: user.first_name = tg_user.first_name; updated_info = True
        if user.last_name != tg_user.last_name: user.last_name = tg_user.last_name; updated_info = True
        if is_new or updated_info or user.referred_by_id: db.commit(); 
            if is_new: db.refresh(user); logger.info(f"User data for {uid} processed/committed.")
        app_name_tg = MINI_APP_NAME or "app"; bot_un = bot.get_me().username; actual_url = f"https://t.me/{bot_un}/{app_name_tg}"
        markup = types.InlineKeyboardMarkup(); web_app = types.WebAppInfo(url=actual_url)
        btn = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app); markup.add(btn)
        bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже, чтобы начать!", reply_markup=markup)
    except Exception as e_start_main: logger.error(f"Error in /start for {message.chat.id}: {e_start_main}", exc_info=True); bot.send_message(message.chat.id, "Error. Try later.")
    finally: db.close()
@bot.message_handler(func=lambda message: True)
def echo_all(message): bot.reply_to(message, "Нажмите /start, чтобы открыть Pusik Gifts.")

bot_polling_started = False; bot_polling_thread = None
def run_bot_polling(): # Same
    global bot_polling_started
    if bot_polling_started: logger.info("Polling already running."); return
    bot_polling_started = True; logger.info("Starting bot polling...")
    for i_poll in range(3): 
        try: bot.remove_webhook(); logger.info("Webhook removed."); break
        except Exception as e_wh_rem: logger.warning(f"Webhook removal attempt {i_poll+1} failed: {e_wh_rem}"); time.sleep(2)
    while bot_polling_started:
        try: logger.debug("Calling infinity_polling..."); bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
        except telebot.apihelper.ApiTelegramException as e_api_poll_run: logger.error(f"TG API Exception (polling): {e_api_poll_run.error_code} - {e_api_poll_run.description}", exc_info=False); 
            if e_api_poll_run.error_code in [401, 409]: logger.error("CRITICAL: Bot token/conflict. Stopping."); bot_polling_started = False
            else: time.sleep(30)
        except ConnectionError as e_conn_poll_run: logger.error(f"ConnectionError (polling): {e_conn_poll_run}", exc_info=False); time.sleep(60)
        except Exception as e_gen_poll_run: logger.error(f"Unexpected error (polling): {type(e_gen_poll_run).__name__} - {e_gen_poll_run}", exc_info=True); time.sleep(60)
        if not bot_polling_started: break
        time.sleep(5) 
    logger.info("Bot polling terminated.")

if __name__ == '__main__': # Same
    if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        logger.info("Main process: Starting bot polling thread."); bot_polling_thread = threading.Thread(target=run_bot_polling, daemon=True); bot_polling_thread.start()
    elif os.environ.get("WERKZEUG_RUN_MAIN") == "true": logger.info("Werkzeug reloader: Bot polling handled by main reloaded instance.")
    logger.info("Starting Flask server..."); app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
