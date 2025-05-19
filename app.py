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
from decimal import Decimal, ROUND_HALF_UP

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base # Removed backref as it's not explicitly used with this relationship setup
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

# Imports for Tonnel Withdrawal - Using PyCryptodome & curl_cffi
from curl_cffi.requests import AsyncSession, RequestsError
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad

# Pytoniq imports
from pytoniq import LiteBalancer
import asyncio

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_NAME = os.environ.get("MINI_APP_NAME", "case")
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24
TONNEL_SENDER_INIT_DATA = os.environ.get("TONNEL_SENDER_INIT_DATA")
TONNEL_GIFT_SECRET = os.environ.get("TONNEL_GIFT_SECRET", "yowtfisthispieceofshitiiit")
API_BASE_URL_FRONTEND = os.environ.get("API_BASE_URL_FRONTEND", "https://case-hznb.onrender.com")

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
if not TONNEL_SENDER_INIT_DATA and os.environ.get("ENABLE_TONNEL_WITHDRAWAL", "true").lower() == "true":
    logger.warning("TONNEL_SENDER_INIT_DATA is not set! Tonnel gift withdrawal will likely fail if enabled.")

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

    # Relationship to the User who referred this current user
    referrer = relationship(
        "User", # Target class name (self)
        remote_side=[id], # The 'id' column of the 'User' table (the referrer's ID)
        foreign_keys=[referred_by_id], # The FK column in *this* table (users.referred_by_id)
        back_populates="referrals_made_explicit",
        uselist=False # A user has at most one referrer
    )

    # Relationship to a list of Users that this current user has referred
    referrals_made_explicit = relationship(
        "User", # Target class name (self)
        # SQLAlchemy infers this connects via the 'referrer' relationship on the other side
        back_populates="referrer",
        # Explicitly state the foreign keys from the perspective of the 'other' User object
        # The 'other' User objects (those referred) will have their 'referred_by_id' pointing to *this* user's id.
        # However, since 'referred_by_id' is on the 'User' table itself, this usually works by SQLAlchemy's inference
        # when `back_populates` is correctly set on both sides.
        # If more explicit control is needed or inference fails:
        # primaryjoin="User.id == User.referred_by_id" # This would define how to join from *this* user to those they referred.
        # Simpler: let SQLAlchemy infer from the `referrer` relationship's `foreign_keys`.
        # The `foreign_keys` argument in `relationship` usually refers to the FKs *on the target side of the join*
        # if not explicitly using `primaryjoin` or `secondaryjoin`.
        # For a one-to-many where the FK is on the "many" side's table (which is `User.referred_by_id`),
        # this is the standard setup.
        # No need for explicit foreign_keys here if `referrer` is correctly set up.
    )


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

# --- (AES Encryption, TonnelGiftSender, Data definitions, Utility functions remain the same) ---
# --- (Flask routes from get_user_data up to, but not including, /start Telegram handler remain the same) ---

# --- INSERT ALL PREVIOUSLY PROVIDED Python code from SALT_SIZE down to the line BEFORE @bot.message_handler(commands=['start']) HERE ---
# This includes:
# - SALT_SIZE, KEY_SIZE, IV_SIZE constants
# - derive_key_and_iv function
# - encrypt_aes_cryptojs_compat function
# - TonnelGiftSender class (full definition)
# - generate_image_filename_from_name function
# - UPDATED_FLOOR_PRICES dictionary
# - cases_data_backend_with_fixed_prices list and its processing loop
# - TON_PRIZE_IMAGE_DEFAULT and slot prize constants
# - slots_data_backend list and its processing loop (including displayPrizes)
# - finalize_slot_prize_pools function
# - calculate_and_log_rtp function
# - populate_initial_data function
# - initial_setup_and_logging function call
# - DEPOSIT_RECIPIENT_ADDRESS_RAW, DEPOSIT_COMMENT, PENDING_DEPOSIT_EXPIRY_MINUTES constants
# - app = Flask(__name__) and CORS setup
# - bot = telebot.TeleBot(BOT_TOKEN)
# - get_db function
# - validate_init_data function
# - @app.route('/') index_route
# - @app.route('/api/get_user_data', methods=['POST']) with corrected invited_friends_count if you choose to use relationship
# - All other /api/ endpoints (open_case, spin_slot, upgrade_item, convert_to_ton, sell_all_items, initiate_deposit, check_blockchain_for_deposit, verify_deposit, get_leaderboard, withdraw_referral_earnings, redeem_promocode, withdraw_item_via_tonnel)

# For brevity, I will re-paste starting from the corrected /api/get_user_data and /api/check_blockchain_for_deposit
# and then the /start handler, assuming other routes are unchanged in their logic.

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

class TonnelGiftSender: # Full class as provided before
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
            except Exception as e_close_session:
                logger.error(f"Error while closing AsyncSession: {e_close_session}")
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
                if 200 <= response_obj.status_code < 300:
                    return {"status": "options_ok"}
                else:
                    err_text_options_resp = await response_obj.text()
                    logger.error(f"Tonnel API OPTIONS request to {url} failed with status {response_obj.status_code}. Response: {err_text_options_resp[:500]}")
                    response_obj.raise_for_status()
                    return {"status": "error", "message": f"OPTIONS request failed: {response_obj.status_code}"}
            response_obj.raise_for_status()
            if response_obj.status_code == 204:
                return None
            content_type = response_obj.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                try:
                    return response_obj.json()
                except json.JSONDecodeError as je_err_inner_req:
                    logger.error(f"Tonnel API JSONDecodeError (inner) for {method} {url}: {je_err_inner_req}", exc_info=False)
                    err_text_json_decode_inner = await response_obj.text()
                    logger.error(f"Response body for inner JSONDecodeError: {err_text_json_decode_inner[:500]}")
                    return {"status": "error", "message": "Invalid JSON in response despite Content-Type application/json", "raw_text": err_text_json_decode_inner[:500]}
            else:
                if is_initial_get:
                    logger.info(f"Tonnel API: Initial GET to {url} successful (Content-Type: {content_type}).")
                    return {"status": "get_ok_non_json"}
                else:
                    responseText_non_json = await response_obj.text()
                    logger.warning(f"Tonnel API {method} {url} - Response is not JSON (Content-Type: {content_type}). Text: {responseText_non_json[:200]}")
                    return {"status": "error", "message": "Response was not JSON as expected", "content_type": content_type, "text_preview": responseText_non_json[:200]}
        except RequestsError as re_err_req:
            logger.error(f"Tonnel API RequestsError ({method} {url}): {re_err_req}", exc_info=False)
            err_text_requests_err_body = ""
            if response_obj is not None:
                try:
                    err_text_requests_err_body = await response_obj.text()
                except:
                    pass
                logger.error(f"Response body for RequestsError (status {response_obj.status_code if response_obj else 'N/A'}): {err_text_requests_err_body[:500]}")
            raise
        except json.JSONDecodeError as je_err_req:
            logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err_req}", exc_info=False)
            err_text_json_outer_body = ""
            if response_obj is not None:
                try:
                    err_text_json_outer_body = await response_obj.text()
                except:
                    pass
                logger.error(f"Response body for outer JSONDecodeError: {err_text_json_outer_body[:500]}")
            raise ValueError(f"Failed to decode JSON from {url}. Content-Type: {response_obj.headers.get('Content-Type', '') if response_obj else 'N/A'}") from je_err_req
        except Exception as e_gen_req:
            logger.error(f"Tonnel API general request error ({method} {url}): {type(e_gen_req).__name__} - {e_gen_req}", exc_info=False)
            err_text_general_body = ""
            if response_obj is not None:
                try:
                    err_text_general_body = await response_obj.text()
                except:
                    pass
                logger.error(f"Response body for general error: {err_text_general_body[:500]}")
            raise

    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int):
        logger.info(f"Attempting Tonnel gift '{gift_item_name}' to user {receiver_telegram_id} using sender auth: {self.authdata[:30]}...")
        if not self.authdata:
            logger.error("TONNEL_SENDER_INIT_DATA not configured.")
            return {"status": "error", "message": "Tonnel sender not configured."}
        try:
            await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)
            logger.info("Tonnel: Initial GET to marketplace.tonnel.network okay.")
            filter_str = json.dumps({"price":{"$exists": True},"refunded":{"$ne": True},"buyer":{"$exists": False},"export_at":{"$exists": True},"gift_name": gift_item_name,"asset":"TON"})
            page_gifts_payload = {"filter": filter_str, "limit":10, "page":1, "sort": '{"price":1,"gift_id":-1}'}
            pg_headers_options = {"Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type", "Origin": "https://tonnel-gift.vercel.app", "Referer": "https://tonnel-gift.vercel.app/"}
            pg_headers_post = {"Content-Type": "application/json", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_options)
            gifts_found_response = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_post, json_payload=page_gifts_payload)
            if not isinstance(gifts_found_response, list):
                err_msg_gifts_fetch = gifts_found_response.get("message", "API error fetching gifts") if isinstance(gifts_found_response, dict) else "Unexpected format for gifts"
                logger.error(f"Tonnel: Failed to fetch gifts for '{gift_item_name}'. Response: {gifts_found_response}")
                return {"status": "error", "message": f"Could not fetch gift list: {err_msg_gifts_fetch}"}
            if not gifts_found_response:
                logger.warning(f"Tonnel: No gifts found for '{gift_item_name}'. Response: {gifts_found_response}")
                return {"status": "error", "message": f"No '{gift_item_name}' gifts currently available on Tonnel."}
            low_gift = gifts_found_response[0]
            logger.info(f"Tonnel: Found gift for '{gift_item_name}': ID {low_gift.get('gift_id')}, Price {low_gift.get('price')} TON")
            user_info_payload = {"authData": self.authdata, "user": receiver_telegram_id}
            ui_common_headers = {"Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            ui_options_headers = {**ui_common_headers, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type"}
            ui_post_headers = {**ui_common_headers, "Content-Type": "application/json"}
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_options_headers)
            user_check_resp = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_post_headers, json_payload=user_info_payload)
            logger.info(f"Tonnel: UserInfo check response: {user_check_resp}")
            if not isinstance(user_check_resp, dict) or user_check_resp.get("status") != "success":
                err_msg_user_check = user_check_resp.get("message", "Tonnel rejected user check.") if isinstance(user_check_resp, dict) else "Unknown user check error."
                logger.warning(f"Tonnel: UserInfo check failed for receiver {receiver_telegram_id}. Resp: {user_check_resp}")
                return {"status": "error", "message": f"Tonnel user check failed: {err_msg_user_check}"}
            time_now_ts_str = f"{int(time.time())}"
            encrypted_ts = encrypt_aes_cryptojs_compat(time_now_ts_str, self.passphrase_secret)
            logger.debug(f"Tonnel: Python AES Encrypted timestamp: {encrypted_ts[:20]}...")
            buy_gift_url = f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}"
            buy_payload = {"anonymously": True, "asset": "TON", "authData": self.authdata, "price": low_gift['price'], "receiver": receiver_telegram_id, "showPrice": False, "timestamp": encrypted_ts}
            buy_common_headers = {"Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/", "Host": "gifts.coffin.meme"}
            buy_options_headers = {**buy_common_headers, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type"}
            buy_post_headers = {**buy_common_headers, "Content-Type": "application/json"}
            await self._make_request(method="OPTIONS", url=buy_gift_url, headers=buy_options_headers)
            purchase_resp = await self._make_request(method="POST", url=buy_gift_url, headers=buy_post_headers, json_payload=buy_payload, timeout=90)
            logger.info(f"Tonnel: BuyGift response for {low_gift['gift_id']} to {receiver_telegram_id}: {purchase_resp}")
            if isinstance(purchase_resp, dict) and purchase_resp.get("status") == "success":
                logger.info(f"Tonnel: Gift '{gift_item_name}' to user {receiver_telegram_id} success.")
                return {"status": "success", "message": f"Gift '{gift_item_name}' sent!", "details": purchase_resp}
            else:
                err_msg_buy_gift = purchase_resp.get("message", "Tonnel rejected purchase.") if isinstance(purchase_resp, dict) else "Unknown purchase error."
                logger.error(f"Tonnel: Failed to send gift '{gift_item_name}'. Resp: {purchase_resp}")
                return {"status": "error", "message": f"Tonnel transfer failed: {err_msg_buy_gift}"}
        except ValueError as ve_send_gift:
             logger.error(f"Tonnel: ValueError during gift sending for '{gift_item_name}' to {receiver_telegram_id}: {ve_send_gift}", exc_info=True)
             return {"status": "error", "message": f"Tonnel API communication error (ValueError): {str(ve_send_gift)}"}
        except RequestsError as re_err_outer_send_gift:
             logger.error(f"Tonnel: RequestsError during gift sending for '{gift_item_name}' to {receiver_telegram_id}: {re_err_outer_send_gift}", exc_info=True)
             return {"status": "error", "message": f"Tonnel network error: {str(re_err_outer_send_gift)}"}
        except Exception as e_send_gift:
            logger.error(f"Tonnel: Unexpected error sending gift '{gift_item_name}' to {receiver_telegram_id}: {type(e_send_gift).__name__} - {e_send_gift}", exc_info=True)
            return {"status": "error", "message": f"Unexpected error during Tonnel withdrawal: {str(e_send_gift)}"}
        finally:
            await self._close_session_if_open()

def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png"
    name_str_rep = name_str.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'
    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    return re.sub(r'-+', '-', cleaned) + '.png'

UPDATED_FLOOR_PRICES = { 'Plush Pepe': 1200.0, 'Neko Helmet': 15.0, 'Sharp Tongue': 17.0, "Durov's Cap": 251.0, 'Voodoo Doll': 9.4, 'Vintage Cigar': 19.7, 'Astral Shard': 50.0, 'Scared Cat': 22.0, 'Swiss Watch': 18.6, 'Perfume Bottle': 38.3, 'Precious Peach': 100.0, 'Toy Bear': 16.3, 'Genie Lamp': 19.3, 'Loot Bag': 25.0, 'Kissed Frog': 14.8, 'Electric Skull': 10.9, 'Diamond Ring': 8.06, 'Mini Oscar': 40.5, 'Party Sparkler': 2.0, 'Homemade Cake': 2.0, 'Cookie Heart': 1.8, 'Jack-in-the-box': 2.0, 'Skull Flower': 3.4, 'Lol Pop': 1.4, 'Hypno Lollipop': 1.4, 'Desk Calendar': 1.4, 'B-Day Candle': 1.4, 'Record Player': 4.0, 'Jelly Bunny': 3.6, 'Tama Gadget': 4.0, 'Snow Globe': 4.0, 'Eternal Rose': 11.0, 'Love Potion': 5.4, 'Top Hat': 6.0 }

cases_data_backend_with_fixed_prices = [
    { 'id': 'lolpop', 'name': 'Lol Pop Stash', 'priceTON': 1.5, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.001 }, { 'name': 'Neko Helmet', 'probability': 0.005 }, { 'name': 'Party Sparkler', 'probability': 0.07 }, { 'name': 'Homemade Cake', 'probability': 0.07 }, { 'name': 'Cookie Heart', 'probability': 0.07 }, { 'name': 'Jack-in-the-box', 'probability': 0.06 }, { 'name': 'Skull Flower', 'probability': 0.023 }, { 'name': 'Lol Pop', 'probability': 0.25 }, { 'name': 'Hypno Lollipop', 'probability': 0.25 }, { 'name': 'Desk Calendar', 'probability': 0.10 }, { 'name': 'B-Day Candle', 'probability': 0.101 } ] },
    { 'id': 'recordplayer', 'name': 'Record Player Vault', 'priceTON': 6.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0012 }, { 'name': 'Record Player', 'probability': 0.40 }, { 'name': 'Lol Pop', 'probability': 0.10 }, { 'name': 'Hypno Lollipop', 'probability': 0.10 }, { 'name': 'Party Sparkler', 'probability': 0.10 }, { 'name': 'Skull Flower', 'probability': 0.10 }, { 'name': 'Jelly Bunny', 'probability': 0.0988 }, { 'name': 'Tama Gadget', 'probability': 0.05 }, { 'name': 'Snow Globe', 'probability': 0.05 } ] },
    { 'id': 'swisswatch', 'name': 'Swiss Watch Box', 'priceTON': 10.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0015 }, { 'name': 'Swiss Watch', 'probability': 0.08 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Eternal Rose', 'probability': 0.05 }, { 'name': 'Electric Skull', 'probability': 0.03 }, { 'name': 'Diamond Ring', 'probability': 0.0395 }, { 'name': 'Record Player', 'probability': 0.20 }, { 'name': 'Love Potion', 'probability': 0.20 }, { 'name': 'Top Hat', 'probability': 0.15 }, { 'name': 'Voodoo Doll', 'probability': 0.149 } ] },
    { 'id': 'perfumebottle', 'name': 'Perfume Chest', 'priceTON': 20.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0018 }, { 'name': 'Perfume Bottle', 'probability': 0.08 }, { 'name': 'Sharp Tongue', 'probability': 0.12 }, { 'name': 'Loot Bag', 'probability': 0.09946 }, { 'name': 'Swiss Watch', 'probability': 0.15 }, { 'name': 'Neko Helmet', 'probability': 0.15 }, { 'name': 'Genie Lamp', 'probability': 0.15 }, { 'name': 'Kissed Frog', 'probability': 0.10 }, { 'name': 'Electric Skull', 'probability': 0.07 }, { 'name': 'Diamond Ring', 'probability': 0.07874 } ] },
    { 'id': 'vintagecigar', 'name': 'Vintage Cigar Safe', 'priceTON': 40.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.002 }, { 'name': 'Perfume Bottle', 'probability': 0.2994 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.12 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Sharp Tongue', 'probability': 0.10 }, { 'name': 'Genie Lamp', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.08 }, { 'name': 'Scared Cat', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.0486 } ] },
    { 'id': 'astralshard', 'name': 'Astral Shard Relic', 'priceTON': 100.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0025 }, { 'name': 'Durov\'s Cap', 'probability': 0.09925 }, { 'name': 'Astral Shard', 'probability': 0.10 }, { 'name': 'Precious Peach', 'probability': 0.10 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Perfume Bottle', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.10 }, { 'name': 'Neko Helmet', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.10 }, { 'name': 'Scared Cat', 'probability': 0.08 }, { 'name': 'Loot Bag', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.04825 } ] },
    { 'id': 'plushpepe', 'name': 'Plush Pepe Hoard', 'priceTON': 200.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.15 }, { 'name': 'Durov\'s Cap', 'probability': 0.25 }, { 'name': 'Astral Shard', 'probability': 0.60 } ] },
    { 'id': 'black', 'name': 'BLACK Singularity', 'isBackgroundCase': True, 'bgImageFilename': 'image-1.png', 'overlayPrizeName': 'Neko Helmet', 'priceTON': 30.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.001 }, { 'name': 'Durov\'s Cap', 'probability': 0.01 }, { 'name': 'Perfume Bottle', 'probability': 0.05 }, { 'name': 'Mini Oscar', 'probability': 0.04 }, { 'name': 'Scared Cat', 'probability': 0.06 }, { 'name': 'Vintage Cigar', 'probability': 0.07 }, { 'name': 'Loot Bag', 'probability': 0.07 }, { 'name': 'Sharp Tongue', 'probability': 0.08 }, { 'name': 'Genie Lamp', 'probability': 0.08 }, { 'name': 'Swiss Watch', 'probability': 0.10 }, { 'name': 'Neko Helmet', 'probability': 0.15 }, { 'name': 'Kissed Frog', 'probability': 0.10 }, { 'name': 'Electric Skull', 'probability': 0.09 }, { 'name': 'Diamond Ring', 'probability': 0.089} ] }
]
cases_data_backend = []
for case_template_item in cases_data_backend_with_fixed_prices:
    processed_case_item = {**case_template_item}
    if not processed_case_item.get('isBackgroundCase'):
        processed_case_item['imageFilename'] = generate_image_filename_from_name(processed_case_item['name'])
    full_prizes_list = []
    for prize_stub_item in processed_case_item['prizes']:
        prize_name_str = prize_stub_item['name']
        full_prizes_list.append({'name': prize_name_str, 'imageFilename': generate_image_filename_from_name(prize_name_str), 'floorPrice': UPDATED_FLOOR_PRICES.get(prize_name_str, 0), 'probability': prize_stub_item['probability']})
    processed_case_item['prizes'] = full_prizes_list
    cases_data_backend.append(processed_case_item)

TON_PRIZE_IMAGE_DEFAULT = "ton_coin.png"
DEFAULT_SLOT_TON_PRIZES = [ {'name': "0.1 TON", 'value': 0.1, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.25 TON", 'value': 0.25, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.5 TON", 'value': 0.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.0 TON", 'value': 1.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.5 TON", 'value': 1.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
PREMIUM_SLOT_TON_PRIZES = [ {'name': "2 TON", 'value': 2.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "3 TON", 'value': 3.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "5 TON", 'value': 5.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "10 TON", 'value': 10.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
ALL_ITEMS_POOL_FOR_SLOTS = [{'name': name_val, 'floorPrice': price_val, 'imageFilename': generate_image_filename_from_name(name_val), 'is_ton_prize': False} for name_val, price_val in UPDATED_FLOOR_PRICES.items()]

slots_data_backend = [
    { 'id': 'default_slot', 'name': 'Default Slot', 'priceTON': 3.0, 'reels_config': 3, 'prize_pool': [],
      'displayPrizes': [ { 'name': 'Lol Pop', 'probability_display': 20}, { 'name': 'Cookie Heart', 'probability_display': 15 }, { 'name': '0.1 TON', 'probability_display': 10 }, {'name': 'Neko Helmet', 'probability_display': 1} ]
    },
    { 'id': 'premium_slot', 'name': 'Premium Slot', 'priceTON': 10.0, 'reels_config': 3, 'prize_pool': [],
      'displayPrizes': [ { 'name': 'Neko Helmet', 'probability_display': 15 }, { 'name': 'Swiss Watch', 'probability_display': 10 }, { 'name': '1.0 TON', 'probability_display': 8 }, {'name': 'Durov\'s Cap', 'probability_display': 0.5} ]
    }
]
for slot_backend_item in slots_data_backend:
    if 'displayPrizes' in slot_backend_item:
        for dp_item_val in slot_backend_item['displayPrizes']:
            if 'imageFilename' not in dp_item_val and not dp_item_val['name'].endswith(" TON"):
                dp_item_val['imageFilename'] = generate_image_filename_from_name(dp_item_val['name'])
            elif dp_item_val['name'].endswith(" TON"):
                 dp_item_val['imageFilename'] = TON_PRIZE_IMAGE_DEFAULT

def finalize_slot_prize_pools():
    global slots_data_backend
    for slot_data_instance in slots_data_backend:
        temp_pool_list = []
        if slot_data_instance['id'] == 'default_slot':
            prob_per_ton_prize = (0.50 / len(DEFAULT_SLOT_TON_PRIZES)) if DEFAULT_SLOT_TON_PRIZES else 0
            for ton_prize_item in DEFAULT_SLOT_TON_PRIZES:
                temp_pool_list.append({**ton_prize_item, 'probability': prob_per_ton_prize})
            item_candidates_list = [item_obj for item_obj in ALL_ITEMS_POOL_FOR_SLOTS if item_obj['floorPrice'] < 15]
            if not item_candidates_list:
                item_candidates_list = ALL_ITEMS_POOL_FOR_SLOTS[:10]
            remaining_prob_for_items = 0.50
            if item_candidates_list:
                prob_per_item_val = remaining_prob_for_items / len(item_candidates_list)
                for item_candidate in item_candidates_list:
                    temp_pool_list.append({**item_candidate, 'probability': prob_per_item_val})
        elif slot_data_instance['id'] == 'premium_slot':
            prob_per_ton_prize = (0.40 / len(PREMIUM_SLOT_TON_PRIZES)) if PREMIUM_SLOT_TON_PRIZES else 0
            for ton_prize_item_prem in PREMIUM_SLOT_TON_PRIZES:
                temp_pool_list.append({**ton_prize_item_prem, 'probability': prob_per_ton_prize})
            item_candidates_list_prem = [item_obj for item_obj in ALL_ITEMS_POOL_FOR_SLOTS if item_obj['floorPrice'] >= 15]
            if not item_candidates_list_prem:
                item_candidates_list_prem = ALL_ITEMS_POOL_FOR_SLOTS[-10:]
            remaining_prob_for_items = 0.60
            if item_candidates_list_prem:
                prob_per_item_prem = remaining_prob_for_items / len(item_candidates_list_prem)
                for item_candidate_prem_val in item_candidates_list_prem:
                    temp_pool_list.append({**item_candidate_prem_val, 'probability': prob_per_item_prem})
        current_total_prob_val = sum(p_val.get('probability', 0) for p_val in temp_pool_list)
        if current_total_prob_val > 0 and abs(current_total_prob_val - 1.0) > 0.001: # Check if sum is not 1
            logger.warning(f"Normalizing probabilities for slot {slot_data_instance['id']}. Original sum: {current_total_prob_val}")
            for p_norm_item in temp_pool_list:
                p_norm_item['probability'] = p_norm_item.get('probability', 0) / current_total_prob_val
        slot_data_instance['prize_pool'] = temp_pool_list

finalize_slot_prize_pools() # Call it after defining slots_data_backend

def calculate_and_log_rtp():
    logger.info("--- RTP Calculations (Based on Current Fixed Prices & Probabilities) ---")
    overall_total_ev_weighted_by_price = Decimal('0')
    overall_total_cost_squared = Decimal('0')
    all_games_data = cases_data_backend + slots_data_backend # Ensure slots_data_backend is ready
    for game_data_item in all_games_data:
        game_id_val = game_data_item['id']
        game_name_val = game_data_item['name']
        price_val_rtp = Decimal(str(game_data_item['priceTON']))
        ev_val = Decimal('0')
        if 'prizes' in game_data_item: # This is for Cases
            for prize_item in game_data_item['prizes']:
                prize_value_calc = Decimal(str(UPDATED_FLOOR_PRICES.get(prize_item['name'], 0)))
                if game_id_val == 'black': # Example: Special multiplier for a specific case
                    prize_value_calc *= Decimal('2.5')
                ev_val += prize_value_calc * Decimal(str(prize_item['probability']))
        elif 'prize_pool' in game_data_item: # This is for Slots
            reels_count = Decimal(str(game_data_item.get('reels_config', 3)))
            for prize_spec_item in game_data_item['prize_pool']:
                value_item_rtp = Decimal(str(prize_spec_item.get('value', prize_spec_item.get('floorPrice', 0))))
                prob_on_reel_val = Decimal(str(prize_spec_item.get('probability', 0)))
                if prize_spec_item.get('is_ton_prize'):
                    # Simplified EV for TON: assumes any landing on payline pays.
                    # This needs to match your actual slot win logic for TON.
                    # If 3 TON symbols are needed, it's (prob_on_reel_val ** reels_count) * value_item_rtp
                    # For now, a rough estimate:
                    ev_val += value_item_rtp * prob_on_reel_val # (Chance of one symbol * value) - adjust if needed
                else: # For item prizes, assuming 3 of a kind on a single payline
                    ev_val += value_item_rtp * (prob_on_reel_val ** reels_count)

        rtp_calc = (ev_val / price_val_rtp) * 100 if price_val_rtp > 0 else Decimal('0')
        dev_cut_calc = 100 - rtp_calc if price_val_rtp > 0 else Decimal('0')
        logger.info(f"Game: {game_name_val:<25} | Price: {price_val_rtp:>6.2f} TON | Est.EV: {ev_val:>6.2f} | Est.RTP: {rtp_calc:>6.2f}% | Est.DevCut: {dev_cut_calc:>6.2f}%")
        if price_val_rtp > 0:
            overall_total_ev_weighted_by_price += ev_val * price_val_rtp
            overall_total_cost_squared += price_val_rtp * price_val_rtp
    if overall_total_cost_squared > 0:
        weighted_avg_rtp_val = (overall_total_ev_weighted_by_price / overall_total_cost_squared) * 100
        logger.info(f"--- Approx. Weighted Avg RTP (by price, for priced games): {weighted_avg_rtp_val:.2f}% ---")
    else:
        logger.info("--- No priced games for overall RTP calculation. ---")

def populate_initial_data():
    db = SessionLocal()
    try:
        for nft_name_val, floor_price_val in UPDATED_FLOOR_PRICES.items():
            nft_exists_check = db.query(NFT).filter(NFT.name == nft_name_val).first()
            if not nft_exists_check:
                db.add(NFT(name=nft_name_val, image_filename=generate_image_filename_from_name(nft_name_val), floor_price=floor_price_val))
            elif nft_exists_check.floor_price != floor_price_val or nft_exists_check.image_filename != generate_image_filename_from_name(nft_name_val):
                nft_exists_check.floor_price = floor_price_val
                nft_exists_check.image_filename = generate_image_filename_from_name(nft_name_val)
        db.commit()
        logger.info("Initial NFT data populated/updated.")
    except Exception as e_populate_data:
        db.rollback()
        logger.error(f"Error populating initial NFT data: {e_populate_data}")
    finally:
        db.close()

def initial_setup_and_logging():
    populate_initial_data()
    db = SessionLocal()
    try:
        if not db.query(PromoCode).filter(PromoCode.code_text == 'Grachev').first():
            db.add(PromoCode(code_text='Grachev', activations_left=10, ton_amount=100.0))
            db.commit()
            logger.info("Promocode 'Grachev' (100 TON, 10 activations) seeded.")
    except Exception as e_seed_promo_code:
        db.rollback()
        logger.error(f"Error seeding Grachev promocode: {e_seed_promo_code}")
    finally:
        db.close()
    calculate_and_log_rtp()

initial_setup_and_logging()

DEPOSIT_RECIPIENT_ADDRESS_RAW = os.environ.get("DEPOSIT_RECIPIENT_ADDRESS_RAW", "YOUR_WALLET_ADDRESS_HERE")
DEPOSIT_COMMENT = os.environ.get("DEPOSIT_COMMENT", "YOUR_UNIQUE_DEPOSIT_COMMENT_HERE")
PENDING_DEPOSIT_EXPIRY_MINUTES = 30

app = Flask(__name__)

PROD_ORIGIN = "https://vasiliy-katsyka.github.io"
NULL_ORIGIN = "null"
LOCAL_DEV_ORIGINS = [
    "http://localhost:5500", "http://127.0.0.1:5500",
    "http://localhost:8000", "http://127.0.0.1:8000",
]
final_allowed_origins_list = list(set([PROD_ORIGIN, NULL_ORIGIN] + LOCAL_DEV_ORIGINS))
CORS(app, resources={r"/api/*": {"origins": final_allowed_origins_list}})

bot = telebot.TeleBot(BOT_TOKEN)

def get_db():
    db_session = SessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()

def validate_init_data(init_data_str: str, bot_token_val: str) -> dict | None:
    try:
        if not init_data_str: return None
        parsed_data_dict = dict(parse_qs(init_data_str))
        if not all(key_check_val in parsed_data_dict for key_check_val in ['hash', 'user', 'auth_date']): return None
        hash_received_val = parsed_data_dict.pop('hash')[0]
        auth_date_ts_val = int(parsed_data_dict['auth_date'][0])
        if (int(dt.now(timezone.utc).timestamp()) - auth_date_ts_val) > AUTH_DATE_MAX_AGE_SECONDS:
            logger.warning("initData expired")
            return None
        data_check_string_parts_list = [f"{k}={parsed_data_dict[k][0]}" for k in sorted(parsed_data_dict.keys())]
        data_check_string_val = "\n".join(data_check_string_parts_list)
        secret_key_val = hmac.new("WebAppData".encode(), bot_token_val.encode(), hashlib.sha256).digest()
        calculated_hash_hex_val = hmac.new(secret_key_val, data_check_string_val.encode(), hashlib.sha256).hexdigest()
        if calculated_hash_hex_val == hash_received_val:
            user_info_str_unquoted_val = unquote(parsed_data_dict['user'][0])
            user_info_final_dict = json.loads(user_info_str_unquoted_val)
            if 'id' not in user_info_final_dict:
                logger.warning("ID missing in user_info_dict")
                return None
            user_info_final_dict['id'] = int(user_info_final_dict['id'])
            return user_info_final_dict
        else:
            logger.warning(f"Hash mismatch: calculated {calculated_hash_hex_val}, received {hash_received_val}")
            return None
    except Exception as e_validate_data_main:
        logger.error(f"initData validation error: {e_validate_data_main}", exc_info=True)
        return None

@app.route('/')
def index_route():
    return "Pusik Gifts App is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth:
        return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            user = User(id=uid, username=auth.get("username"), first_name=auth.get("first_name"), last_name=auth.get("last_name"), referral_code=f"ref_{uid}_{random.randint(1000,9999)}")
            db.add(user)
            db.commit()
            db.refresh(user)
        
        # Use the relationship to count invited friends
        invited_friends_count = len(user.referrals_made_explicit) if user.referrals_made_explicit else 0
        # Or, if you prefer the direct query (less ORM-idiomatic but also works):
        # invited_friends_count = db.query(User).filter(User.referred_by_id == uid).count()

        inv = [{"id": i.id, "name": i.nft.name if i.nft else i.item_name_override, "imageFilename": i.nft.image_filename if i.nft else i.item_image_override, "floorPrice": i.nft.floor_price if i.nft else i.current_value, "currentValue": i.current_value, "upgradeMultiplier": i.upgrade_multiplier, "variant": i.variant, "is_ton_prize": i.is_ton_prize, "obtained_at": i.obtained_at.isoformat() if i.obtained_at else None} for i in user.inventory]
        
        return jsonify({
            "id": user.id, "username": user.username, "first_name": user.first_name, 
            "last_name": user.last_name, "tonBalance": user.ton_balance, 
            "starBalance": user.star_balance, "inventory": inv, 
            "referralCode": user.referral_code, 
            "referralEarningsPending": user.referral_earnings_pending, 
            "total_won_ton": user.total_won_ton, 
            "invited_friends_count": invited_friends_count
        })
    finally:
        db.close()

# ... (All other API routes: open_case, spin_slot, upgrade_item, etc. using the corrected variable names from the prior "full Python" response)
# Ensure to use the variable renaming convention I applied previously (e.g. item_to_upgrade instead of item)
# For check_blockchain_for_deposit, ensure the referral logic uses user_to_credit_deposit.referrer

async def check_blockchain_for_deposit(pending_deposit_to_check: PendingDeposit, db_session_for_check): # Renamed args
    lite_balancer_client = None
    try:
        lite_balancer_client = LiteBalancer.from_mainnet_config(trust_level=2)
        await lite_balancer_client.start_up()
        
        transactions_list = await lite_balancer_client.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=30)
        
        for transaction in transactions_list:
            if transaction.in_msg and \
               transaction.in_msg.is_internal and \
               transaction.in_msg.info.value_coins == pending_deposit_to_check.final_amount_nano_ton and \
               transaction.now > int((pending_deposit_to_check.created_at - timedelta(minutes=10)).timestamp()):
                
                comment_slice_from_tx = transaction.in_msg.body.begin_parse()
                if comment_slice_from_tx.remaining_bits >= 32 and comment_slice_from_tx.load_uint(32) == 0:
                    try:
                        tx_comment_text = comment_slice_from_tx.load_snake_string()
                        if tx_comment_text == pending_deposit_to_check.expected_comment:
                            user_to_credit_deposit = db_session_for_check.query(User).filter(User.id == pending_deposit_to_check.user_id).with_for_update().first()
                            if not user_to_credit_deposit:
                                pending_deposit_to_check.status = 'failed'
                                db_session_for_check.commit()
                                logger.error(f"User ID {pending_deposit_to_check.user_id} for deposit {pending_deposit_to_check.id} not found.")
                                return {"status": "error", "message": "User associated with deposit vanished."}
                            
                            user_to_credit_deposit.ton_balance = float(Decimal(str(user_to_credit_deposit.ton_balance)) + Decimal(str(pending_deposit_to_check.original_amount_ton)))
                            
                            # Corrected: Use the 'referrer' relationship to get the referrer object
                            if user_to_credit_deposit.referrer: # This will be the referrer User object or None
                                referrer_for_bonus = user_to_credit_deposit.referrer
                                # It's good practice to lock the referrer row if you're updating it,
                                # though SQLAlchemy might handle this if part of same session. For explicit lock:
                                db_session_for_check.refresh(referrer_for_bonus, with_for_update=True)

                                bonus_amount = (Decimal(str(pending_deposit_to_check.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                referrer_for_bonus.referral_earnings_pending = float(Decimal(str(referrer_for_bonus.referral_earnings_pending)) + bonus_amount)
                                logger.info(f"Credited referrer {referrer_for_bonus.id} with {bonus_amount:.2f} TON for deposit by {user_to_credit_deposit.id}")
                            elif user_to_credit_deposit.referred_by_id: # Fallback if relationship didn't load for some reason, but referrer_id exists
                                logger.warning(f"User {user_to_credit_deposit.id} has referred_by_id {user_to_credit_deposit.referred_by_id}, but referrer relationship was None. Attempting direct query.")
                                direct_referrer = db_session_for_check.query(User).filter(User.id == user_to_credit_deposit.referred_by_id).with_for_update().first()
                                if direct_referrer:
                                    bonus_amount = (Decimal(str(pending_deposit_to_check.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                    direct_referrer.referral_earnings_pending = float(Decimal(str(direct_referrer.referral_earnings_pending)) + bonus_amount)
                                    logger.info(f"Credited referrer (direct query) {direct_referrer.id} with {bonus_amount:.2f} TON for deposit by {user_to_credit_deposit.id}")
                                else:
                                    logger.error(f"Referrer with ID {user_to_credit_deposit.referred_by_id} not found for user {user_to_credit_deposit.id} despite referred_by_id being set.")


                            pending_deposit_to_check.status = 'completed'
                            db_session_for_check.commit()
                            return {"status": "success", "message": "Deposit confirmed and credited!", "new_balance_ton": user_to_credit_deposit.ton_balance}
                    except Exception as e_parse_comment:
                        logger.debug(f"Could not parse comment or non-matching comment for tx {transaction.hash}: {e_parse_comment}")
                        pass
        
        if pending_deposit_to_check.expires_at <= dt.now(timezone.utc) and pending_deposit_to_check.status == 'pending':
            pending_deposit_to_check.status = 'expired'
            db_session_for_check.commit()
            return {"status": "expired", "message": "Deposit has expired and was not found."}
        
        return {"status": "pending", "message": "Transaction not yet confirmed on the blockchain."}
    except Exception as e_blockchain_check:
        logger.error(f"Error during blockchain check for deposit: {e_blockchain_check}", exc_info=True)
        return {"status": "error", "message": "An error occurred while checking the blockchain."}
    finally:
        if lite_balancer_client:
            await lite_balancer_client.close_all()

# --- (All other API routes as before) ---
# Copy all previous API routes here, make sure variable names are consistent if you renamed them

# Example of one more route for consistency:
@app.route('/api/withdraw_referral_earnings', methods=['POST'])
def withdraw_referral_earnings_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    db = next(get_db())
    try:
        user_withdrawing_referrals = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user_withdrawing_referrals: return jsonify({"error": "User not found"}), 404
        
        if user_withdrawing_referrals.referral_earnings_pending > 0:
            amount_withdrawn = Decimal(str(user_withdrawing_referrals.referral_earnings_pending))
            user_withdrawing_referrals.ton_balance = float(Decimal(str(user_withdrawing_referrals.ton_balance)) + amount_withdrawn)
            user_withdrawing_referrals.referral_earnings_pending = 0.0
            db.commit()
            return jsonify({"status": "success", "message": f"{amount_withdrawn:.2f} TON from referral earnings moved to main balance.", "new_balance_ton": user_withdrawing_referrals.ton_balance, "new_referral_earnings_pending": 0.0})
        else:
            return jsonify({"status": "no_earnings", "message": "No referral earnings available to withdraw."})
    except Exception as e_withdraw_referral:
        db.rollback()
        logger.error(f"Error withdrawing referral earnings: {e_withdraw_referral}", exc_info=True)
        return jsonify({"error": "An internal error occurred."}), 500
    finally:
        db.close()


# --- Telegram Bot Handlers (Corrected /start) ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"/start from {message.chat.id} ({message.from_user.username}) text: '{message.text}'")
    db = next(get_db())
    try:
        user_id_from_message = message.chat.id
        telegram_user_object = message.from_user
        user_record = db.query(User).filter(User.id == user_id_from_message).first()
        was_user_created_now = False

        if not user_record:
            was_user_created_now = True
            # Ensure referral_code is unique if somehow a collision occurs (very unlikely with user_id in it)
            potential_ref_code = f"ref_{user_id_from_message}_{random.randint(1000,9999)}"
            while db.query(User).filter(User.referral_code == potential_ref_code).first():
                potential_ref_code = f"ref_{user_id_from_message}_{random.randint(1000,9999)}"

            user_record = User(
                id=user_id_from_message, 
                username=telegram_user_object.username, 
                first_name=telegram_user_object.first_name, 
                last_name=telegram_user_object.last_name, 
                referral_code=potential_ref_code
            )
            db.add(user_record)
            logger.info(f"New user {user_id_from_message} created with ref code {user_record.referral_code}")
        
        # Process referral from deep link
        try:
            message_command_parts = message.text.split(' ')
            if len(message_command_parts) > 1 and message_command_parts[1].startswith('startapp='):
                start_parameter = message_command_parts[1].split('=')[1]
                if start_parameter.startswith('ref_'):
                    referral_code_from_link = start_parameter
                    if (was_user_created_now or not user_record.referred_by_id) and \
                       (user_record.referral_code != referral_code_from_link): # Prevent self-referral
                        
                        referrer_user_record = db.query(User).filter(User.referral_code == referral_code_from_link).first() # Removed User.id != user_record.id as self-ref check is done with codes
                        if referrer_user_record and referrer_user_record.id != user_record.id: # Double check not self
                            user_record.referred_by_id = referrer_user_record.id
                            logger.info(f"User {user_id_from_message} was referred by {referrer_user_record.id} (code: {referral_code_from_link}).")
                            try: 
                                bot.send_message(referrer_user_record.id, f"🎉 Your friend {user_record.first_name or user_record.username or user_record.id} joined using your referral link!")
                            except Exception as e_notify_referrer: 
                                logger.warning(f"Failed to notify referrer {referrer_user_record.id}: {e_notify_referrer}")
                        elif referrer_user_record and referrer_user_record.id == user_record.id:
                            logger.info(f"User {user_id_from_message} attempted self-referral with code {referral_code_from_link}.")
                        else:
                             logger.warning(f"Referral code {referral_code_from_link} from deep link not found for any other user.")
        except Exception as e_process_referral:
            logger.error(f"Error processing /start command parameter for user {user_id_from_message}: {e_process_referral}")

        were_fields_updated = False
        if user_record.username != telegram_user_object.username: 
            user_record.username = telegram_user_object.username
            were_fields_updated = True
        if user_record.first_name != telegram_user_object.first_name: 
            user_record.first_name = telegram_user_object.first_name
            were_fields_updated = True
        if user_record.last_name != telegram_user_object.last_name: 
            user_record.last_name = telegram_user_object.last_name
            were_fields_updated = True
        
        # Check if referred_by_id was just set by inspecting the session's state for the object
        is_referred_by_id_dirty = False
        insp = db.inspect(user_record)
        if insp.attrs.referred_by_id.history.has_changes():
            is_referred_by_id_dirty = True
            
        if was_user_created_now or were_fields_updated or is_referred_by_id_dirty:
            try:
                db.commit()
                if was_user_created_now:
                    db.refresh(user_record)
                logger.info(f"User data for {user_id_from_message} was committed/updated (new: {was_user_created_now}, updated: {were_fields_updated}, ref_dirty: {is_referred_by_id_dirty}).")
            except IntegrityError as ie: # Catch if referral_code somehow still collided
                db.rollback()
                logger.error(f"IntegrityError committing user {user_id_from_message}: {ie}. Possibly duplicate referral_code.")
                if was_user_created_now: # If it was a new user, try to give them a new code or handle error
                    user_record.referral_code = f"ref_{user_id_from_message}_{random.randint(10000,99999)}" # Try again with more random
                    try:
                        db.add(user_record) # Re-add if rolled back
                        db.commit()
                        db.refresh(user_record)
                        logger.info(f"User {user_id_from_message} committed with new ref_code after collision.")
                    except Exception as e_retry_commit:
                        db.rollback()
                        logger.error(f"Failed to commit user {user_id_from_message} even after ref_code regeneration: {e_retry_commit}")
                        bot.send_message(message.chat.id, "Sorry, there was a critical error setting up your profile. Please try /start again.")
                        return
            except Exception as e_commit_start_user:
                db.rollback()
                logger.error(f"Error committing user data for {user_id_from_message} during /start: {e_commit_start_user}")
                user_check_after_commit_fail = db.query(User).filter(User.id == user_id_from_message).first()
                if not user_check_after_commit_fail and was_user_created_now:
                     bot.send_message(message.chat.id, "Sorry, there was an error setting up your profile. Please try the /start command again.")
                     return

        mini_app_button_url = f"https://t.me/{bot.get_me().username}/{MINI_APP_NAME or 'app'}"
        if not MINI_APP_NAME:
            logger.warning("MINI_APP_NAME environment variable is not set. The Mini App button link might be incorrect or default to 'app'.")
            
        welcome_markup = types.InlineKeyboardMarkup()
        mini_app_web_app_info = types.WebAppInfo(url=mini_app_button_url)
        launch_app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=mini_app_web_app_info)
        welcome_markup.add(launch_app_button)
        bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже, чтобы начать!", reply_markup=welcome_markup)
    except Exception as e_start_command_handler:
        logger.error(f"General error in /start command handler for chat {message.chat.id}: {e_start_command_handler}", exc_info=True)
        bot.send_message(message.chat.id, "An unexpected error occurred while processing your request. Please try again later.")
    finally:
        db.close()

@bot.message_handler(func=lambda message: True)
def echo_all_messages(message):
    bot.reply_to(message, "Пожалуйста, используйте команду /start, чтобы открыть приложение Pusik Gifts.")

bot_polling_active_flag = False
bot_polling_execution_thread = None

def run_bot_polling_loop():
    global bot_polling_active_flag
    if bot_polling_active_flag: 
        logger.info("Bot polling process is already active in this thread/process.")
        return
    bot_polling_active_flag = True
    logger.info("Attempting to start bot polling...")
    for attempt_num in range(3): 
        try: 
            bot.remove_webhook()
            logger.info("Any existing webhook was successfully removed.")
            break
        except Exception as e_remove_webhook: 
            logger.warning(f"Attempt {attempt_num+1} to remove webhook failed: {e_remove_webhook}")
            if attempt_num < 2:
                time.sleep(2)
            else:
                logger.error("Failed to remove webhook after multiple attempts. Polling might conflict if a webhook is set elsewhere.")
    while bot_polling_active_flag:
        try:
            logger.info("Bot is now starting infinity_polling...")
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
            logger.info("infinity_polling has completed a cycle or was interrupted.") 
        except telebot.apihelper.ApiTelegramException as e_telegram_api:
            logger.error(f"Telegram API Exception encountered in polling loop: Code {e_telegram_api.error_code} - {e_telegram_api.description}", exc_info=False)
            if e_telegram_api.error_code == 401 or e_telegram_api.error_code == 409:
                logger.error("CRITICAL: Bot token seems invalid or there's a conflict with another bot instance. Stopping polling.")
                bot_polling_active_flag = False
            else: 
                logger.info("Waiting for 30 seconds before retrying polling after API exception.")
                time.sleep(30)
        except ConnectionError as e_network_connection: 
            logger.error(f"Network ConnectionError encountered in polling loop: {e_network_connection}", exc_info=False)
            logger.info("Waiting for 60 seconds before retrying polling due to network issue.")
            time.sleep(60)
        except Exception as e_generic_polling: 
            logger.error(f"An unexpected critical error occurred in the polling loop: {type(e_generic_polling).__name__} - {e_generic_polling}", exc_info=True)
            logger.info("Waiting for 60 seconds before retrying polling after unexpected error.")
            time.sleep(60)
        if not bot_polling_active_flag:
            logger.info("Bot polling flag is false, exiting polling loop.")
            break
        if bot_polling_active_flag: 
            logger.info("Brief pause before restarting infinity_polling.")
            time.sleep(5) 
    logger.info("Bot polling loop has officially terminated.")

if __name__ == '__main__':
    if BOT_TOKEN and not bot_polling_active_flag and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        logger.info("Main process detected. Initializing and starting bot polling thread.")
        bot_polling_execution_thread = threading.Thread(target=run_bot_polling_loop, daemon=True)
        bot_polling_execution_thread.start()
    elif os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        logger.info("Werkzeug reloader process detected. Bot polling will be managed by the primary reloaded instance.")
    logger.info(f"Starting Flask application server on host 0.0.0.0, port {os.environ.get('PORT', 5000)}...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
