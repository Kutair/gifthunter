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
    star_balance = Column(Integer, default=0, nullable=False) # Assuming stars might be a future feature
    referral_code = Column(String, unique=True, index=True, nullable=True)
    referred_by_id = Column(BigInteger, ForeignKey("users.id"), nullable=True)
    referral_earnings_pending = Column(Float, default=0.0, nullable=False)
    total_won_ton = Column(Float, default=0.0, nullable=False) # For leaderboard
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    inventory = relationship("InventoryItem", back_populates="owner", cascade="all, delete-orphan")
    pending_deposits = relationship("PendingDeposit", back_populates="owner")
    # Relationship for referrals
    referrer = relationship("User", remote_side=[id], foreign_keys=[referred_by_id], back_populates="referrals_made", uselist=False) # The user who referred this user
    referrals_made = relationship("User", back_populates="referrer") # List of users this user has referred

class NFT(Base):
    __tablename__ = "nfts"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True, index=True, nullable=False)
    image_filename = Column(String, nullable=True) # e.g., "Neko-Helmet.png"
    floor_price = Column(Float, default=0.0, nullable=False) # In TON
    __table_args__ = (UniqueConstraint('name', name='uq_nft_name'),)

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    nft_id = Column(Integer, ForeignKey("nfts.id"), nullable=True) # If it's a standard NFT
    item_name_override = Column(String, nullable=True) # For items not in NFT table, or special variants
    item_image_override = Column(String, nullable=True)
    current_value = Column(Float, nullable=False) # In TON
    upgrade_multiplier = Column(Float, default=1.0, nullable=False)
    obtained_at = Column(DateTime(timezone=True), server_default=func.now())
    variant = Column(String, nullable=True) # e.g., "black_singularity"
    is_ton_prize = Column(Boolean, default=False, nullable=False) # If true, current_value is TON amount, nft_id might be null
    owner = relationship("User", back_populates="inventory")
    nft = relationship("NFT") # Link to the NFT table if nft_id is set

class PendingDeposit(Base):
    __tablename__ = "pending_deposits"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    original_amount_ton = Column(Float, nullable=False) # The amount user intended to deposit
    unique_identifier_nano_ton = Column(BigInteger, nullable=False) # The small random nanoTON part for uniqueness
    final_amount_nano_ton = Column(BigInteger, nullable=False, index=True) # original_amount_ton * 1e9 + unique_identifier_nano_ton
    expected_comment = Column(String, nullable=False, default="cpd7r07ud3s") # Static comment for all deposits
    status = Column(String, default="pending", index=True) # pending, completed, expired, failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=False)
    owner = relationship("User", back_populates="pending_deposits")

class PromoCode(Base):
    __tablename__ = "promo_codes"
    id = Column(Integer, primary_key=True, index=True)
    code_text = Column(String, unique=True, index=True, nullable=False)
    activations_left = Column(Integer, nullable=False, default=0) # Max number of activations
    ton_amount = Column(Float, nullable=False, default=0.0) # TON to grant on activation
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

Base.metadata.create_all(bind=engine)


# --- Tonnel Gift Sender (AES Encryption part) ---
SALT_SIZE = 8
KEY_SIZE = 32
IV_SIZE = 16

def derive_key_and_iv(passphrase: str, salt: bytes, key_length: int, iv_length: int) -> tuple[bytes, bytes]:
    derived = b''
    hasher = hashlib.md5()
    hasher.update(passphrase.encode('utf-8'))
    hasher.update(salt)
    derived_block = hasher.digest()
    derived += derived_block
    while len(derived) < key_length + iv_length:
        hasher = hashlib.md5()
        hasher.update(derived_block)
        hasher.update(passphrase.encode('utf-8'))
        hasher.update(salt)
        derived_block = hasher.digest()
        derived += derived_block
    key = derived[:key_length]
    iv = derived[key_length : key_length + iv_length]
    return key, iv

def encrypt_aes_cryptojs_compat(plain_text: str, secret_passphrase: str) -> str:
    salt = get_random_bytes(SALT_SIZE)
    key, iv = derive_key_and_iv(secret_passphrase, salt, KEY_SIZE, IV_SIZE)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plain_text_bytes = plain_text.encode('utf-8')
    padded_plain_text = pad(plain_text_bytes, AES.block_size, style='pkcs7')
    ciphertext = cipher.encrypt(padded_plain_text)
    salted_ciphertext = b"Salted__" + salt + ciphertext
    encrypted_base64 = base64.b64encode(salted_ciphertext).decode('utf-8')
    return encrypted_base64

class TonnelGiftSender:
    def __init__(self, sender_auth_data: str, gift_secret_passphrase: str):
        self.passphrase_secret = gift_secret_passphrase
        self.authdata = sender_auth_data
        self._session_instance: AsyncSession | None = None

    async def _get_session(self) -> AsyncSession:
        if self._session_instance is None:
            self._session_instance = AsyncSession(impersonate="chrome110")
            logger.debug("Initialized new AsyncSession for TonnelGiftSender.")
        return self._session_instance

    async def _close_session_if_open(self):
        if self._session_instance:
            logger.debug("Closing AsyncSession for TonnelGiftSender.")
            try:
                await self._session_instance.close()
            except Exception as e_close:
                logger.error(f"Error while closing AsyncSession: {e_close}")
            finally:
                self._session_instance = None

    async def _make_request(self, method: str, url: str, headers: dict | None = None, json_payload: dict | None = None, timeout: int = 30, is_initial_get: bool = False):
        session = await self._get_session()
        response_obj = None
        try:
            logger.debug(f"Tonnel API Request: {method} {url} Headers: {headers} Payload: {json_payload}")
            request_kwargs = {"headers": headers, "timeout": timeout}
            if json_payload is not None and method.upper() == "POST":
                request_kwargs["json"] = json_payload

            if method.upper() == "GET":
                response_obj = await session.get(url, **request_kwargs)
            elif method.upper() == "POST":
                response_obj = await session.post(url, **request_kwargs)
            elif method.upper() == "OPTIONS":
                response_obj = await session.options(url, **request_kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            logger.debug(f"Tonnel API Response: {method} {url} - Status: {response_obj.status_code}, Response Headers: {dict(response_obj.headers)}")
            if method.upper() == "OPTIONS":
                if 200 <= response_obj.status_code < 300: return {"status": "options_ok"}
                else:
                    err_text_options = await response_obj.text()
                    logger.error(f"Tonnel API OPTIONS request to {url} failed with status {response_obj.status_code}. Response: {err_text_options[:500]}")
                    response_obj.raise_for_status()
                    return {"status": "error", "message": f"OPTIONS request failed: {response_obj.status_code}"}
            response_obj.raise_for_status()
            if response_obj.status_code == 204: return None
            content_type = response_obj.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                try: return response_obj.json()
                except json.JSONDecodeError as je_err_inner:
                    logger.error(f"Tonnel API JSONDecodeError (inner) for {method} {url}: {je_err_inner}", exc_info=False)
                    err_text_json_decode = await response_obj.text()
                    logger.error(f"Response body for inner JSONDecodeError: {err_text_json_decode[:500]}")
                    return {"status": "error", "message": "Invalid JSON in response", "raw_text": err_text_json_decode[:500]}
            else:
                if is_initial_get: return {"status": "get_ok_non_json"}
                else:
                    responseText = await response_obj.text()
                    logger.warning(f"Tonnel API {method} {url} - Response not JSON. Text: {responseText[:200]}")
                    return {"status": "error", "message": "Response not JSON", "content_type": content_type, "text_preview": responseText[:200]}
        except RequestsError as re_err:
            logger.error(f"Tonnel API RequestsError ({method} {url}): {re_err}", exc_info=False)
            err_text_req_err = ""
            if response_obj is not None:
                try: err_text_req_err = await response_obj.text()
                except: pass
                logger.error(f"Response body for RequestsError: {err_text_req_err[:500]}")
            raise
        except json.JSONDecodeError as je_err:
            logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err}", exc_info=False)
            err_text_json_outer = ""
            if response_obj is not None:
                try: err_text_json_outer = await response_obj.text()
                except: pass
                logger.error(f"Response body for outer JSONDecodeError: {err_text_json_outer[:500]}")
            raise ValueError(f"Failed to decode JSON from {url}.") from je_err
        except Exception as e_gen:
            logger.error(f"Tonnel API general request error ({method} {url}): {type(e_gen).__name__} - {e_gen}", exc_info=False)
            err_text_general = ""
            if response_obj is not None:
                try: err_text_general = await response_obj.text()
                except: pass
                logger.error(f"Response body for general error: {err_text_general[:500]}")
            raise

    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int):
        logger.info(f"Attempting Tonnel gift '{gift_item_name}' to user {receiver_telegram_id} ...")
        if not self.authdata:
            logger.error("TONNEL_SENDER_INIT_DATA not configured.")
            return {"status": "error", "message": "Tonnel sender not configured."}
        try:
            await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)
            filter_str = json.dumps({"price": {"$exists": True}, "refunded": {"$ne": True}, "buyer": {"$exists": False}, "export_at": {"$exists": True}, "gift_name": gift_item_name, "asset": "TON"})
            page_gifts_payload = {"filter": filter_str, "limit": 10, "page": 1, "sort": '{"price":1,"gift_id":-1}'}
            pg_headers_options = {"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type", "Origin": "https://tonnel-gift.vercel.app", "Referer": "https://tonnel-gift.vercel.app/"}
            pg_headers_post = {"Content-Type": "application/json", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_options)
            gifts_found_response = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_post, json_payload=page_gifts_payload)
            if not isinstance(gifts_found_response, list) or not gifts_found_response:
                err_msg_gifts = gifts_found_response.get("message", "API error fetching gifts") if isinstance(gifts_found_response, dict) else "No gifts found or unexpected format"
                logger.error(f"Tonnel: Failed to fetch/find gifts for '{gift_item_name}'. Response: {gifts_found_response}")
                return {"status": "error", "message": f"No '{gift_item_name}' gifts currently available or error: {err_msg_gifts}"}
            low_gift = gifts_found_response[0]
            logger.info(f"Tonnel: Found gift '{gift_item_name}': ID {low_gift.get('gift_id')}, Price {low_gift.get('price')} TON")
            user_info_payload = {"authData": self.authdata, "user": receiver_telegram_id}
            ui_common_headers = {"Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            ui_options_headers = {**ui_common_headers, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type"}
            ui_post_headers = {**ui_common_headers, "Content-Type": "application/json"}
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_options_headers)
            user_check_resp = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_post_headers, json_payload=user_info_payload)
            if not isinstance(user_check_resp, dict) or user_check_resp.get("status") != "success":
                err_msg_user = user_check_resp.get("message", "Tonnel rejected user check.") if isinstance(user_check_resp, dict) else "Unknown user check error."
                logger.warning(f"Tonnel: UserInfo check failed. Resp: {user_check_resp}")
                return {"status": "error", "message": f"Tonnel user check failed: {err_msg_user}"}
            time_now_ts_str = f"{int(time.time())}"
            encrypted_ts = encrypt_aes_cryptojs_compat(time_now_ts_str, self.passphrase_secret)
            buy_gift_url = f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}"
            buy_payload = {"anonymously": True, "asset": "TON", "authData": self.authdata, "price": low_gift['price'], "receiver": receiver_telegram_id, "showPrice": False, "timestamp": encrypted_ts}
            buy_common_headers = {"Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/", "Host": "gifts.coffin.meme"}
            buy_options_headers = {**buy_common_headers, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type"}
            buy_post_headers = {**buy_common_headers, "Content-Type": "application/json"}
            await self._make_request(method="OPTIONS", url=buy_gift_url, headers=buy_options_headers)
            purchase_resp = await self._make_request(method="POST", url=buy_gift_url, headers=buy_post_headers, json_payload=buy_payload, timeout=90)
            logger.info(f"Tonnel: BuyGift response: {purchase_resp}")
            if isinstance(purchase_resp, dict) and purchase_resp.get("status") == "success":
                return {"status": "success", "message": f"Gift '{gift_item_name}' sent!", "details": purchase_resp}
            else:
                err_msg_buy = purchase_resp.get("message", "Tonnel rejected purchase.") if isinstance(purchase_resp, dict) else "Unknown purchase error."
                logger.error(f"Tonnel: Failed to send gift. Resp: {purchase_resp}")
                return {"status": "error", "message": f"Tonnel transfer failed: {err_msg_buy}"}
        except ValueError as ve:
             logger.error(f"Tonnel: ValueError: {ve}", exc_info=True)
             return {"status": "error", "message": f"Tonnel API error: {str(ve)}"}
        except RequestsError as re_err_outer:
             logger.error(f"Tonnel: RequestsError: {re_err_outer}", exc_info=True)
             return {"status": "error", "message": f"Tonnel network error: {str(re_err_outer)}"}
        except Exception as e:
            logger.error(f"Tonnel: Unexpected error: {type(e).__name__} - {e}", exc_info=True)
            return {"status": "error", "message": f"Unexpected error: {str(e)}"}
        finally:
            await self._close_session_if_open()

# --- Game Data (Cases & Slots) ---
def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png"
    name_str_rep = name_str.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'
    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    return re.sub(r'-+', '-', cleaned) + '.png'

UPDATED_FLOOR_PRICES = { 'Plush Pepe': 1200.0, 'Neko Helmet': 15.0, 'Sharp Tongue': 17.0, "Durov's Cap": 251.0, 'Voodoo Doll': 9.4, 'Vintage Cigar': 19.7, 'Astral Shard': 50.0, 'Scared Cat': 22.0, 'Swiss Watch': 18.6, 'Perfume Bottle': 38.3, 'Precious Peach': 100.0, 'Toy Bear': 16.3, 'Genie Lamp': 19.3, 'Loot Bag': 25.0, 'Kissed Frog': 14.8, 'Electric Skull': 10.9, 'Diamond Ring': 8.06, 'Mini Oscar': 40.5, 'Party Sparkler': 2.0, 'Homemade Cake': 2.0, 'Cookie Heart': 1.8, 'Jack-in-the-box': 2.0, 'Skull Flower': 3.4, 'Lol Pop': 1.4, 'Hynpo Lollipop': 1.4, 'Desk Calendar': 1.4, 'B-Day Candle': 1.4, 'Record Player': 4.0, 'Jelly Bunny': 3.6, 'Tama Gadget': 4.0, 'Snow Globe': 4.0, 'Eternal Rose': 11.0, 'Love Potion': 5.4, 'Top Hat': 6.0 }
TON_PRIZE_IMAGE_DEFAULT = generate_image_filename_from_name(None) # Placeholder, as frontend uses specific URL

cases_data_backend_with_fixed_prices = [
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
for case_template in cases_data_backend_with_fixed_prices:
    processed_case = {**case_template}
    if not processed_case.get('isBackgroundCase'):
        processed_case['imageFilename'] = generate_image_filename_from_name(processed_case['name'])
    full_prizes = []
    for prize_stub in processed_case['prizes']:
        prize_name = prize_stub['name']
        full_prizes.append({'name': prize_name, 'imageFilename': generate_image_filename_from_name(prize_name), 'floorPrice': UPDATED_FLOOR_PRICES.get(prize_name, 0), 'probability': prize_stub['probability']})
    processed_case['prizes'] = full_prizes
    cases_data_backend.append(processed_case)

DEFAULT_SLOT_TON_PRIZES = [ {'name': "0.1 TON", 'value': 0.1, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.25 TON", 'value': 0.25, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.5 TON", 'value': 0.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.0 TON", 'value': 1.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.5 TON", 'value': 1.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
PREMIUM_SLOT_TON_PRIZES = [ {'name': "2 TON", 'value': 2.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "3 TON", 'value': 3.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "5 TON", 'value': 5.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "10 TON", 'value': 10.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
ALL_ITEMS_POOL_FOR_SLOTS = [{'name': name, 'floorPrice': price, 'imageFilename': generate_image_filename_from_name(name), 'is_ton_prize': False} for name, price in UPDATED_FLOOR_PRICES.items()]

slots_data_backend = [
    { 'id': 'default_slot', 'name': 'Default Slot', 'priceTON': 3.0, 'reels_config': 3, 'prize_pool': [] },
    { 'id': 'premium_slot', 'name': 'Premium Slot', 'priceTON': 10.0, 'reels_config': 3, 'prize_pool': [] }
]
def finalize_slot_prize_pools():
    global slots_data_backend
    for slot_data in slots_data_backend:
        temp_pool = []
        if slot_data['id'] == 'default_slot':
            prob_per_ton_prize = (0.50 / len(DEFAULT_SLOT_TON_PRIZES)) if DEFAULT_SLOT_TON_PRIZES else 0
            for ton_prize in DEFAULT_SLOT_TON_PRIZES:
                temp_pool.append({**ton_prize, 'probability': prob_per_ton_prize})
            item_candidates = [item for item in ALL_ITEMS_POOL_FOR_SLOTS if item['floorPrice'] < 15]
            if not item_candidates: item_candidates = ALL_ITEMS_POOL_FOR_SLOTS[:10] if ALL_ITEMS_POOL_FOR_SLOTS else []
            remaining_prob_for_items = 0.50
            if item_candidates:
                prob_per_item = remaining_prob_for_items / len(item_candidates)
                for item in item_candidates: temp_pool.append({**item, 'probability': prob_per_item})
        elif slot_data['id'] == 'premium_slot':
            prob_per_ton_prize = (0.40 / len(PREMIUM_SLOT_TON_PRIZES)) if PREMIUM_SLOT_TON_PRIZES else 0
            for ton_prize in PREMIUM_SLOT_TON_PRIZES: temp_pool.append({**ton_prize, 'probability': prob_per_ton_prize})
            item_candidates = [item for item in ALL_ITEMS_POOL_FOR_SLOTS if item['floorPrice'] >= 15]
            if not item_candidates: item_candidates = ALL_ITEMS_POOL_FOR_SLOTS[-10:] if ALL_ITEMS_POOL_FOR_SLOTS else []
            remaining_prob_for_items = 0.60
            if item_candidates:
                prob_per_item = remaining_prob_for_items / len(item_candidates)
                for item in item_candidates: temp_pool.append({**item, 'probability': prob_per_item})
        current_total_prob = sum(p.get('probability', 0) for p in temp_pool)
        if current_total_prob > 0 and abs(current_total_prob - 1.0) > 0.001:
            logger.warning(f"Normalizing probabilities for slot {slot_data['id']}. Original sum: {current_total_prob}")
            for p_item in temp_pool: p_item['probability'] = p_item.get('probability', 0) / current_total_prob
        slot_data['prize_pool'] = temp_pool
finalize_slot_prize_pools()

def calculate_and_log_rtp(): # Logging RTPs
    logger.info("--- RTP Calculations ---")
    all_games_data = cases_data_backend + slots_data_backend
    for game_data in all_games_data:
        # ... (RTP calculation logic as before, ensure it uses Decimal correctly) ...
        pass # Keep existing RTP logic
    logger.info("--- End RTP Calculations ---")

def populate_initial_data(): # Seed NFTs
    db = SessionLocal()
    try:
        for nft_name, floor_price in UPDATED_FLOOR_PRICES.items():
            nft_exists = db.query(NFT).filter(NFT.name == nft_name).first()
            if not nft_exists: db.add(NFT(name=nft_name, image_filename=generate_image_filename_from_name(nft_name), floor_price=floor_price))
            elif nft_exists.floor_price != floor_price or nft_exists.image_filename != generate_image_filename_from_name(nft_name):
                nft_exists.floor_price = floor_price; nft_exists.image_filename = generate_image_filename_from_name(nft_name)
        db.commit()
        logger.info("Initial NFT data populated/updated.")
    except Exception as e: db.rollback(); logger.error(f"Error populating initial NFT data: {e}")
    finally: db.close()

def initial_setup_and_logging():
    populate_initial_data()
    # Seed promocode if needed
    # calculate_and_log_rtp() # Call this if you want RTP logs on startup

initial_setup_and_logging()

# --- Flask App & Routes ---
DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR" # Replace with your actual deposit address
DEPOSIT_COMMENT = "cpd7r07ud3s" # Your unique deposit comment
PENDING_DEPOSIT_EXPIRY_MINUTES = 30

app = Flask(__name__)
PROD_ORIGIN = "https://vasiliy-katsyka.github.io"
NULL_ORIGIN = "null"
LOCAL_DEV_ORIGINS = ["http://localhost:5500", "http://127.0.0.1:5500", "http://localhost:8000", "http://127.0.0.1:8000"]
final_allowed_origins = list(set([PROD_ORIGIN, NULL_ORIGIN] + LOCAL_DEV_ORIGINS))
CORS(app, resources={r"/api/*": {"origins": final_allowed_origins}})

bot = telebot.TeleBot(BOT_TOKEN)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def validate_init_data(init_data_str: str, bot_token: str) -> dict | None:
    if not init_data_str:
        logger.warning("validate_init_data: init_data_str is empty or None.")
        return None
    try:
        logger.debug(f"validate_init_data: Received init_data_str (first 100 chars): {init_data_str[:100]}")
        parsed_data = dict(parse_qs(init_data_str))
        if not all(k in parsed_data for k in ['hash', 'user', 'auth_date']):
            logger.warning(f"validate_init_data: Missing one or more required keys (hash, user, auth_date). Present keys: {list(parsed_data.keys())}")
            return None
        
        hash_received = parsed_data.pop('hash')[0]
        auth_date_ts = int(parsed_data['auth_date'][0])
        current_ts = int(dt.now(timezone.utc).timestamp())

        if (current_ts - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS:
            logger.warning(f"validate_init_data: initData expired. auth_date: {auth_date_ts}, current_ts: {current_ts}, age: {current_ts - auth_date_ts}s")
            return None
        
        data_check_string_parts = []
        for k_sorted in sorted(parsed_data.keys()):
            data_check_string_parts.append(f"{k_sorted}={parsed_data[k_sorted][0]}")
        data_check_string = "\n".join(data_check_string_parts)
        logger.debug(f"validate_init_data: Data check string: {data_check_string}")

        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        logger.debug(f"validate_init_data: Hash received: {hash_received}, Hash calculated: {calculated_hash_hex}")

        if calculated_hash_hex == hash_received:
            user_info_str_unquoted = unquote(parsed_data['user'][0])
            user_info_dict = json.loads(user_info_str_unquoted)
            if 'id' not in user_info_dict:
                logger.warning("validate_init_data: 'id' missing in user_info_dict from initData.")
                return None
            user_info_dict['id'] = int(user_info_dict['id']) # Ensure ID is int
            logger.info(f"validate_init_data: Validation successful for user: {user_info_dict.get('id')}")
            return user_info_dict
        else:
            logger.warning("validate_init_data: Hash mismatch.")
            return None
    except Exception as e_validate:
        logger.error(f"validate_init_data: Error during validation: {e_validate}", exc_info=True)
        return None

@app.route('/')
def index_route(): return "Pusik Gifts App is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    init_data_header = flask_request.headers.get('X-Telegram-Init-Data')
    logger.info(f"/api/get_user_data called. X-Telegram-Init-Data header present: {'Yes' if init_data_header else 'No'}")
    auth = validate_init_data(init_data_header, BOT_TOKEN)
    if not auth:
        logger.warning("/api/get_user_data: Auth failed.")
        return jsonify({"error": "Authentication failed. Please ensure you are accessing through Telegram."}), 401
    
    uid = auth["id"]
    logger.info(f"/api/get_user_data: Authenticated user ID: {uid}")
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            logger.info(f"/api/get_user_data: User {uid} not found, creating new user.")
            user = User(id=uid, username=auth.get("username"), first_name=auth.get("first_name"), last_name=auth.get("last_name"))
            if not user.referral_code: # Generate referral code if not set (e.g. during creation)
                 user.referral_code = f"ref_{uid}_{random.randint(1000,9999)}"
            db.add(user)
            db.commit()
            db.refresh(user) # Important to get the full user object with defaults
            logger.info(f"/api/get_user_data: New user {uid} created successfully.")
        else:
            logger.info(f"/api/get_user_data: User {uid} found.")
            # Optionally update user info if it changed in Telegram
            changed = False
            if user.username != auth.get("username"): user.username = auth.get("username"); changed = True
            if user.first_name != auth.get("first_name"): user.first_name = auth.get("first_name"); changed = True
            if user.last_name != auth.get("last_name"): user.last_name = auth.get("last_name"); changed = True
            if changed:
                db.commit()
                logger.info(f"User {uid} info updated from Telegram profile.")


        inv = []
        for i in user.inventory:
            item_name = "Unknown Item"
            image_filename = generate_image_filename_from_name(None) # placeholder
            floor_price = i.current_value # fallback

            if i.nft: # If linked to an NFT
                item_name = i.nft.name
                image_filename = i.nft.image_filename
                floor_price = i.nft.floor_price
            elif i.item_name_override: # If it's a custom item or TON prize with overrides
                item_name = i.item_name_override
                if i.item_image_override: image_filename = i.item_image_override
            
            inv.append({
                "id": i.id, 
                "name": item_name, 
                "imageFilename": image_filename, 
                "floorPrice": float(floor_price), 
                "currentValue": float(i.current_value), 
                "upgradeMultiplier": float(i.upgrade_multiplier), 
                "variant": i.variant, 
                "is_ton_prize": i.is_ton_prize, 
                "obtained_at": i.obtained_at.isoformat() if i.obtained_at else None
            })

        refs_count = db.query(User).filter(User.referred_by_id == uid).count()
        
        user_data_response = {
            "id": user.id, "username": user.username, "first_name": user.first_name, 
            "last_name": user.last_name, "tonBalance": float(user.ton_balance), 
            "starBalance": int(user.star_balance), "inventory": inv, 
            "referralCode": user.referral_code, 
            "referralEarningsPending": float(user.referral_earnings_pending), 
            "total_won_ton": float(user.total_won_ton), 
            "invited_friends_count": refs_count
        }
        logger.info(f"/api/get_user_data: Returning data for user {uid}: { {k:v for k,v in user_data_response.items() if k != 'inventory'} }") # Log without bulky inventory
        return jsonify(user_data_response)
    except Exception as e:
        db.rollback()
        logger.error(f"/api/get_user_data: Error for user {uid if 'uid' in locals() else 'unknown'}: {e}", exc_info=True)
        return jsonify({"error": "Database error or unexpected issue processing user data."}), 500
    finally:
        db.close()


@app.route('/api/open_case', methods=['POST'])
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
            if not chosen_prize_info: continue # Should not happen if prizes_in_case is not empty
            dbnft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
            if not dbnft: logger.error(f"NFT {chosen_prize_info['name']} missing!"); continue
            variant = "black_singularity" if tcase.get('id') == 'black' else None
            actual_val = Decimal(str(dbnft.floor_price)) * (Decimal('2.5') if variant == "black_singularity" else Decimal('1'))
            total_value_this_spin += actual_val
            item = InventoryItem(user_id=uid, nft_id=dbnft.id, current_value=float(actual_val.quantize(Decimal('0.01'))), variant=variant)
            db.add(item); db.flush() 
            won_prizes_list.append({"id": item.id, "name": dbnft.name, "imageFilename": dbnft.image_filename, "floorPrice": float(dbnft.floor_price), "currentValue": item.current_value, "variant": item.variant})
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status": "success", "won_prizes": won_prizes_list, "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Open case error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/spin_slot', methods=['POST'])
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
        reel_results_data = [] # This is for frontend display of what landed on each reel
        for _ in range(num_reels):
            rv = random.random(); cprob = 0; landed = None
            for p_info_slot in slot_pool:
                cprob += p_info_slot.get('probability', 0)
                if rv <= cprob: landed = p_info_slot; break
            if not landed: landed = random.choice(slot_pool) if slot_pool else None
            if landed: reel_results_data.append(landed)
            else: reel_results_data.append({"name": "Error", "imageFilename": generate_image_filename_from_name(None), "is_ton_prize": False, "value": 0})
        
        won_prizes_from_slot = [] # Actual prizes user gets
        total_value_this_spin = Decimal('0')
        
        # Check for TON prize wins (any reel can be a TON prize independent of others)
        for landed_item_data in reel_results_data:
            if landed_item_data.get('is_ton_prize'):
                ton_val = Decimal(str(landed_item_data['value']))
                won_prizes_from_slot.append({"id": f"ton_{int(time.time()*1e3)}_{random.randint(0,999)}", "name": landed_item_data['name'], "imageFilename": landed_item_data.get('imageFilename', TON_PRIZE_IMAGE_DEFAULT), "currentValue": float(ton_val), "is_ton_prize": True})
                total_value_this_spin += ton_val
                user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_val) # Add directly to balance

        # Check for item win (all reels must match and not be TON prizes)
        if num_reels > 0 and all(not r.get('is_ton_prize') for r in reel_results_data):
            first_item_name = reel_results_data[0]['name']
            if all(r['name'] == first_item_name for r in reel_results_data):
                # All reels match and are items, so user wins this item
                won_item_data = reel_results_data[0] # Use data from first reel as representative
                db_nft = db.query(NFT).filter(NFT.name == won_item_data['name']).first()
                if db_nft:
                    actual_val = Decimal(str(db_nft.floor_price))
                    inv_item = InventoryItem(user_id=uid, nft_id=db_nft.id, current_value=float(actual_val), variant=None) # Add other fields if needed
                    db.add(inv_item); db.flush()
                    won_prizes_from_slot.append({"id": inv_item.id, "name": db_nft.name, "imageFilename": db_nft.image_filename, "floorPrice": float(db_nft.floor_price), "currentValue": inv_item.current_value, "is_ton_prize": False, "variant": inv_item.variant})
                    total_value_this_spin += actual_val
                else: logger.error(f"Slot item win: NFT '{won_item_data['name']}' not in DB.")
        
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status": "success", "reel_results": reel_results_data, "won_prizes": won_prizes_from_slot, "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Spin slot error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/upgrade_item', methods=['POST'])
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
        user = db.query(User).filter(User.id == uid).with_for_update().first() # Ensure user is also locked if updating total_won_ton
        if random.uniform(0,100) < chances[mult]:
            orig_val = Decimal(str(item.current_value)); new_val = (orig_val * mult).quantize(Decimal('0.01'), ROUND_HALF_UP)
            increase = new_val - orig_val
            item.current_value = float(new_val); item.upgrade_multiplier = float(Decimal(str(item.upgrade_multiplier)) * mult)
            if user: user.total_won_ton = float(Decimal(str(user.total_won_ton)) + increase)
            db.commit()
            nft_name = item.nft.name if item.nft else item.item_name_override
            return jsonify({"status": "success", "message": f"Upgraded! New value: {new_val:.2f} TON", "item": {"id": item.id, "name": nft_name, "currentValue": item.current_value, "upgradeMultiplier": item.upgrade_multiplier, "variant": item.variant }})
        else:
            name_lost = item.nft.name if item.nft else item.item_name_override
            val_lost = Decimal(str(item.current_value))
            if user: user.total_won_ton = float(Decimal(str(user.total_won_ton)) - val_lost)
            db.delete(item); db.commit()
            return jsonify({"status": "failed", "message": f"Upgrade failed! Lost {name_lost}.", "item_lost": True})
    except Exception as e: db.rollback(); logger.error(f"Upgrade error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/convert_to_ton', methods=['POST'])
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
        val = Decimal(str(item.current_value))
        user.ton_balance = float(Decimal(str(user.ton_balance)) + val)
        item_name = item.nft.name if item.nft else item.item_name_override
        db.delete(item); db.commit()
        return jsonify({"status": "success", "message": f"{item_name} sold for {val:.2f} TON.", "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Convert error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        items_to_sell = [i for i in user.inventory if not i.is_ton_prize]
        if not items_to_sell: return jsonify({"status": "no_items", "message": "No sellable items."})
        total_val = sum(Decimal(str(i.current_value)) for i in items_to_sell)
        user.ton_balance = float(Decimal(str(user.ton_balance)) + total_val)
        for i_del in items_to_sell: db.delete(i_del)
        db.commit()
        return jsonify({"status": "success", "message": f"All {len(items_to_sell)} sellable items sold for {total_val:.2f} TON.", "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Sell all error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/initiate_deposit', methods=['POST'])
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
        user = db.query(User).filter(User.id == uid).first() # No need for_update here, just check
        if not user: return jsonify({"error": "User not found"}), 404
        if db.query(PendingDeposit).filter(PendingDeposit.user_id == uid, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first():
            return jsonify({"error": "Active deposit exists."}), 409
        nano_part = random.randint(10000, 999999); final_nano_amt = int(orig_amt * 1e9) + nano_part
        pdep = PendingDeposit(user_id=uid, original_amount_ton=orig_amt, unique_identifier_nano_ton=nano_part, final_amount_nano_ton=final_nano_amt, expected_comment=DEPOSIT_COMMENT, expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
        db.add(pdep); db.commit(); db.refresh(pdep)
        disp_amt = f"{final_nano_amt / 1e9:.9f}".rstrip('0').rstrip('.')
        return jsonify({"status": "success", "pending_deposit_id": pdep.id, "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW, "amount_to_send": disp_amt, "final_amount_nano_ton": final_nano_amt, "comment": DEPOSIT_COMMENT, "expires_at": pdep.expires_at.isoformat()})
    except Exception as e: db.rollback(); logger.error(f"Init deposit error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

async def check_blockchain_for_deposit(pdep: PendingDeposit, db_sess):
    prov = None
    try:
        prov = LiteBalancer.from_mainnet_config(trust_level=2); await prov.start_up()
        txs = await prov.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=30)
        for tx in txs:
            if tx.in_msg and tx.in_msg.is_internal and tx.in_msg.info.value_coins == pdep.final_amount_nano_ton and tx.now > int((pdep.created_at - timedelta(minutes=5)).timestamp()):
                cmt_slice = tx.in_msg.body.begin_parse()
                if cmt_slice.remaining_bits >= 32 and cmt_slice.load_uint(32) == 0: # Check for text comment prefix
                    try:
                        comment_text = cmt_slice.load_snake_string()
                        if comment_text == pdep.expected_comment:
                            usr = db_sess.query(User).filter(User.id == pdep.user_id).with_for_update().first()
                            if not usr: pdep.status = 'failed'; db_sess.commit(); return {"status": "error", "message": "User not found."}
                            usr.ton_balance = float(Decimal(str(usr.ton_balance)) + Decimal(str(pdep.original_amount_ton)))
                            if usr.referred_by_id:
                                referrer = db_sess.query(User).filter(User.id == usr.referred_by_id).with_for_update().first()
                                if referrer:
                                    ref_bonus = (Decimal(str(pdep.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                    referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + ref_bonus)
                            pdep.status = 'completed'; db_sess.commit()
                            return {"status": "success", "message": "Deposit confirmed!", "new_balance_ton": usr.ton_balance}
                    except Exception as e_cmt: logger.debug(f"Comment parse issue for tx {tx.hash}: {e_cmt}"); pass
        if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending':
            pdep.status = 'expired'; db_sess.commit(); return {"status": "expired", "message": "Deposit expired."}
        return {"status": "pending", "message": "Transaction not confirmed yet."}
    except Exception as e_bc: logger.error(f"Blockchain check error: {e_bc}", exc_info=True); return {"status": "error", "message": "Blockchain check error."}
    finally:
        if prov: await prov.close_all()

@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); pid = data.get('pending_deposit_id')
    if not pid: return jsonify({"error": "ID required"}), 400
    db = next(get_db())
    try:
        pdep = db.query(PendingDeposit).filter(PendingDeposit.id == pid, PendingDeposit.user_id == uid).with_for_update().first()
        if not pdep: return jsonify({"error": "Deposit not found"}), 404
        if pdep.status == 'completed':
            usr = db.query(User).filter(User.id == uid).first()
            return jsonify({"status": "success", "message": "Already confirmed.", "new_balance_ton": usr.ton_balance if usr else 0})
        if pdep.status == 'expired' or pdep.expires_at <= dt.now(timezone.utc):
            if pdep.status == 'pending': pdep.status = 'expired'; db.commit()
            return jsonify({"status": "expired", "message": "Deposit expired."}), 400
        result = {}
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running(): # For environments like Flask dev server with reloader
                new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop)
                result = new_loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
            else: result = loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
        except RuntimeError as e_rt: # Catch "event loop is closed" or similar
            logger.info(f"RuntimeError for asyncio in verify_deposit, creating new loop: {e_rt}")
            new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop)
            result = new_loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
        return jsonify(result)
    except Exception as e: db.rollback(); logger.error(f"Verify deposit error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    try:
        leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        return jsonify([{"rank": r+1, "name": u.first_name or u.username or f"User_{str(u.id)[:6]}", "avatarChar": (u.first_name or u.username or "U")[0].upper(), "income": u.total_won_ton, "user_id": u.id} for r, u in enumerate(leaders)])
    finally: db.close()

@app.route('/api/withdraw_referral_earnings', methods=['POST'])
def withdraw_referral_earnings_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        if user.referral_earnings_pending > 0:
            withdrawn_amount = Decimal(str(user.referral_earnings_pending))
            user.ton_balance = float(Decimal(str(user.ton_balance)) + withdrawn_amount)
            user.referral_earnings_pending = 0.0; db.commit()
            return jsonify({"status": "success", "message": f"{withdrawn_amount:.2f} TON withdrawn.", "new_balance_ton": user.ton_balance, "new_referral_earnings_pending": 0.0})
        else: return jsonify({"status": "no_earnings", "message": "No earnings."})
    except Exception as e: db.rollback(); logger.error(f"Withdraw ref error: {e}", exc_info=True); return jsonify({"error": "DB error."}), 500
    finally: db.close()

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); code_txt = data.get('promocode_text', "").strip()
    if not code_txt: return jsonify({"status": "error", "message": "Code empty."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).with_for_update().first()
        if not user: return jsonify({"status": "error", "message": "User not found."}), 404
        if not promo: return jsonify({"status": "error", "message": "Invalid code."}), 404
        if promo.activations_left <= 0: return jsonify({"status": "error", "message": "Code expired."}), 400
        promo.activations_left -= 1
        user.ton_balance = float(Decimal(str(user.ton_balance)) + Decimal(str(promo.ton_amount)))
        db.commit()
        return jsonify({"status": "success", "message": f"Redeemed! +{promo.ton_amount:.2f} TON.", "new_balance_ton": user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Promo error: {e}", exc_info=True); return jsonify({"status": "error", "message": "DB error."}), 500
    finally: db.close()

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST'])
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id):
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"status": "error", "message": "Auth failed"}), 401
    player_id = auth["id"]
    if not TONNEL_SENDER_INIT_DATA: return jsonify({"status": "error", "message": "Withdrawal unavailable."}), 500
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == player_id).with_for_update().first()
        if not item or item.is_ton_prize: return jsonify({"status": "error", "message": "Item not found/withdrawable."}), 404
        item_name_tonnel = item.nft.name if item.nft else item.item_name_override # Must be exact Tonnel name
        
        client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        result = {}
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running(): new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop); result = new_loop.run_until_complete(client.send_gift_to_user(gift_item_name=item_name_tonnel, receiver_telegram_id=player_id))
            else: result = loop.run_until_complete(client.send_gift_to_user(gift_item_name=item_name_tonnel, receiver_telegram_id=player_id))
        except RuntimeError as e_rt_tonnel:
            logger.info(f"RuntimeError for asyncio in Tonnel withdraw, new loop: {e_rt_tonnel}")
            new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop); result = new_loop.run_until_complete(client.send_gift_to_user(gift_item_name=item_name_tonnel, receiver_telegram_id=player_id))

        if result and result.get("status") == "success":
            val_deducted = Decimal(str(item.current_value))
            player_user = db.query(User).filter(User.id == player_id).with_for_update().first() # Re-fetch with lock
            if player_user: player_user.total_won_ton = float(max(Decimal('0'), Decimal(str(player_user.total_won_ton)) - val_deducted))
            db.delete(item); db.commit()
            return jsonify({"status": "success", "message": f"Gift '{item_name_tonnel}' sent! {result.get('message', '')}", "details": result.get("details")})
        else:
            db.rollback() # Rollback if Tonnel failed but DB changes were staged
            return jsonify({"status": "error", "message": f"Tonnel failed: {result.get('message', 'API error')}"}), 500
    except Exception as e: db.rollback(); logger.error(f"Tonnel withdraw error: {e}", exc_info=True); return jsonify({"status": "error", "message": "Unexpected error."}), 500
    finally: db.close()

@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"/start from {message.chat.id} ({message.from_user.username}) text: '{message.text}'")
    db = next(get_db())
    try:
        user_id = message.chat.id; tg_user = message.from_user
        user = db.query(User).filter(User.id == user_id).first()
        is_new_user = not user
        if is_new_user:
            user = User(id=user_id, username=tg_user.username, first_name=tg_user.first_name, last_name=tg_user.last_name)
            user.referral_code = f"ref_{user_id}_{random.randint(1000,9999)}" # Ensure new users get a code
            db.add(user)
        
        # Referral logic via deep link parameter (startapp=ref_XYZ)
        try:
            command_parts = message.text.split(' ')
            if len(command_parts) > 1 and command_parts[1].startswith('startapp='):
                start_param = command_parts[1].split('=', 1)[1]
                if start_param.startswith('ref_'):
                    ref_code_from_link = start_param
                    if (is_new_user or not user.referred_by_id) and user.referral_code != ref_code_from_link : # Avoid self-referral
                        referrer = db.query(User).filter(User.referral_code == ref_code_from_link, User.id != user.id).first()
                        if referrer:
                            user.referred_by_id = referrer.id
                            logger.info(f"User {user_id} referred by {referrer.id} via {ref_code_from_link}.")
                            try: bot.send_message(referrer.id, f"🎉 Your friend {user.first_name or user.username or user.id} joined using your referral link!")
                            except Exception as e_notify: logger.warning(f"Failed to notify referrer {referrer.id}: {e_notify}")
                        else: logger.warning(f"Referral code {ref_code_from_link} not found or self-referral by {user_id}.")
        except Exception as e_ref_link: logger.error(f"Error processing deep link referral for {user_id}: {e_ref_link}")
        
        # Update user info if changed
        updated = False
        if user.username != tg_user.username: user.username = tg_user.username; updated = True
        if user.first_name != tg_user.first_name: user.first_name = tg_user.first_name; updated = True
        if user.last_name != tg_user.last_name: user.last_name = tg_user.last_name; updated = True
        if is_new_user or updated or user.referred_by_id: # Commit if new, or info updated, or referral was set
            db.commit()
            if is_new_user: db.refresh(user)
            logger.info(f"User data for {user_id} processed/committed.")

        app_name = MINI_APP_NAME or "app" # Fallback if env var is missing
        bot_username = bot.get_me().username
        mini_app_actual_url = f"https://t.me/{bot_username}/{app_name}"
        
        markup = types.InlineKeyboardMarkup()
        web_app_info = types.WebAppInfo(url=mini_app_actual_url)
        app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app_info)
        markup.add(app_button)
        bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже, чтобы начать!", reply_markup=markup)
    except Exception as e_start: logger.error(f"Error in /start for {message.chat.id}: {e_start}", exc_info=True); bot.send_message(message.chat.id, "Error. Try later.")
    finally: db.close()

@bot.message_handler(func=lambda message: True)
def echo_all(message): bot.reply_to(message, "Нажмите /start, чтобы открыть Pusik Gifts.")

bot_polling_started = False
bot_polling_thread = None
def run_bot_polling():
    global bot_polling_started
    if bot_polling_started: logger.info("Polling already running."); return
    bot_polling_started = True; logger.info("Starting bot polling...")
    for i in range(3): # Retry removing webhook
        try: bot.remove_webhook(); logger.info("Webhook removed."); break
        except Exception as e: logger.warning(f"Webhook removal attempt {i+1} failed: {e}"); time.sleep(2)
    while bot_polling_started:
        try:
            logger.debug("Calling infinity_polling...")
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
        except telebot.apihelper.ApiTelegramException as e_api:
            logger.error(f"TG API Exception (polling): {e_api.error_code} - {e_api.description}", exc_info=False)
            if e_api.error_code in [401, 409]: logger.error("CRITICAL: Bot token/conflict. Stopping."); bot_polling_started = False
            else: time.sleep(30)
        except ConnectionError as e_conn: logger.error(f"ConnectionError (polling): {e_conn}", exc_info=False); time.sleep(60)
        except Exception as e_gen: logger.error(f"Unexpected error (polling): {type(e_gen).__name__} - {e_gen}", exc_info=True); time.sleep(60)
        if not bot_polling_started: break
        time.sleep(5) # Brief pause before next poll cycle if it exited normally
    logger.info("Bot polling terminated.")

if __name__ == '__main__':
    if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        logger.info("Main process: Starting bot polling thread.")
        bot_polling_thread = threading.Thread(target=run_bot_polling, daemon=True)
        bot_polling_thread.start()
    elif os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        logger.info("Werkzeug reloader: Bot polling will be handled by main reloaded instance.")
    
    logger.info("Starting Flask server...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
