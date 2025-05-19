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
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
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

class User(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=False)
    username = Column(String, nullable=True, index=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    ton_balance = Column(Float, default=0.0, nullable=False)
    star_balance = Column(Integer, default=0, nullable=False)
    referral_code = Column(String, unique=True, index=True, nullable=True)
    
    referred_by_id = Column(BigInteger, ForeignKey("users.id", name="fk_user_referred_by"), nullable=True)
    
    referral_earnings_pending = Column(Float, default=0.0, nullable=False)
    total_won_ton = Column(Float, default=0.0, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    inventory = relationship("InventoryItem", back_populates="owner", cascade="all, delete-orphan")
    pending_deposits = relationship("PendingDeposit", back_populates="owner")

    referrer = relationship(
        "User", 
        remote_side=[id], 
        foreign_keys=[referred_by_id], 
        back_populates="referrals_made_explicit",
        uselist=False 
    )

    referrals_made_explicit = relationship(
        "User", 
        back_populates="referrer",
        foreign_keys="User.referred_by_id"
    )

class NFT(Base):
    __tablename__ = "nfts"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True, index=True, nullable=False)
    image_filename = Column(String, nullable=True) # Relative path or full URL if stored externally
    floor_price = Column(Float, default=0.0, nullable=False)
    __table_args__ = (UniqueConstraint('name', name='uq_nft_name'),)

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE", name="fk_inventory_user"), nullable=False)
    nft_id = Column(Integer, ForeignKey("nfts.id", name="fk_inventory_nft"), nullable=True)
    item_name_override = Column(String, nullable=True)
    item_image_override = Column(String, nullable=True) # Can be full URL if image source is external
    current_value = Column(Float, nullable=False)
    upgrade_multiplier = Column(Float, default=1.0, nullable=False)
    obtained_at = Column(DateTime(timezone=True), server_default=func.now())
    variant = Column(String, nullable=True)
    is_ton_prize = Column(Boolean, default=False, nullable=False)
    image_filename_is_full_url = Column(Boolean, default=False, nullable=False) # New field to indicate if item_image_override is a full URL

    owner = relationship("User", back_populates="inventory")
    nft = relationship("NFT")

class PendingDeposit(Base):
    __tablename__ = "pending_deposits"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE", name="fk_pendingdeposit_user"), nullable=False)
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
                except: pass
                logger.error(f"Response body for RequestsError (status {response_obj.status_code if response_obj else 'N/A'}): {err_text_requests_err_body[:500]}")
            raise
        except json.JSONDecodeError as je_err_req:
            logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err_req}", exc_info=False)
            err_text_json_outer_body = ""
            if response_obj is not None:
                try:
                    err_text_json_outer_body = await response_obj.text()
                except: pass
                logger.error(f"Response body for outer JSONDecodeError: {err_text_json_outer_body[:500]}")
            raise ValueError(f"Failed to decode JSON from {url}. Content-Type: {response_obj.headers.get('Content-Type', '') if response_obj else 'N/A'}") from je_err_req
        except Exception as e_gen_req:
            logger.error(f"Tonnel API general request error ({method} {url}): {type(e_gen_req).__name__} - {e_gen_req}", exc_info=False)
            err_text_general_body = ""
            if response_obj is not None:
                try:
                    err_text_general_body = await response_obj.text()
                except: pass
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

cases_data_backend_with_fixed_prices = [ # BLACK Singularity case removed
    { 'id': 'lolpop', 'name': 'Lol Pop Stash', 'priceTON': 1.5, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.001 }, { 'name': 'Neko Helmet', 'probability': 0.005 }, { 'name': 'Party Sparkler', 'probability': 0.07 }, { 'name': 'Homemade Cake', 'probability': 0.07 }, { 'name': 'Cookie Heart', 'probability': 0.07 }, { 'name': 'Jack-in-the-box', 'probability': 0.06 }, { 'name': 'Skull Flower', 'probability': 0.023 }, { 'name': 'Lol Pop', 'probability': 0.25 }, { 'name': 'Hypno Lollipop', 'probability': 0.25 }, { 'name': 'Desk Calendar', 'probability': 0.10 }, { 'name': 'B-Day Candle', 'probability': 0.101 } ] },
    { 'id': 'recordplayer', 'name': 'Record Player Vault', 'priceTON': 6.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0012 }, { 'name': 'Record Player', 'probability': 0.40 }, { 'name': 'Lol Pop', 'probability': 0.10 }, { 'name': 'Hypno Lollipop', 'probability': 0.10 }, { 'name': 'Party Sparkler', 'probability': 0.10 }, { 'name': 'Skull Flower', 'probability': 0.10 }, { 'name': 'Jelly Bunny', 'probability': 0.0988 }, { 'name': 'Tama Gadget', 'probability': 0.05 }, { 'name': 'Snow Globe', 'probability': 0.05 } ] },
    { 'id': 'swisswatch', 'name': 'Swiss Watch Box', 'priceTON': 10.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0015 }, { 'name': 'Swiss Watch', 'probability': 0.08 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Eternal Rose', 'probability': 0.05 }, { 'name': 'Electric Skull', 'probability': 0.03 }, { 'name': 'Diamond Ring', 'probability': 0.0395 }, { 'name': 'Record Player', 'probability': 0.20 }, { 'name': 'Love Potion', 'probability': 0.20 }, { 'name': 'Top Hat', 'probability': 0.15 }, { 'name': 'Voodoo Doll', 'probability': 0.149 } ] },
    { 'id': 'perfumebottle', 'name': 'Perfume Chest', 'priceTON': 20.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0018 }, { 'name': 'Perfume Bottle', 'probability': 0.08 }, { 'name': 'Sharp Tongue', 'probability': 0.12 }, { 'name': 'Loot Bag', 'probability': 0.09946 }, { 'name': 'Swiss Watch', 'probability': 0.15 }, { 'name': 'Neko Helmet', 'probability': 0.15 }, { 'name': 'Genie Lamp', 'probability': 0.15 }, { 'name': 'Kissed Frog', 'probability': 0.10 }, { 'name': 'Electric Skull', 'probability': 0.07 }, { 'name': 'Diamond Ring', 'probability': 0.07874 } ] },
    { 'id': 'vintagecigar', 'name': 'Vintage Cigar Safe', 'priceTON': 40.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.002 }, { 'name': 'Perfume Bottle', 'probability': 0.2994 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.12 }, { 'name': 'Neko Helmet', 'probability': 0.10 }, { 'name': 'Sharp Tongue', 'probability': 0.10 }, { 'name': 'Genie Lamp', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.08 }, { 'name': 'Scared Cat', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.0486 } ] },
    { 'id': 'astralshard', 'name': 'Astral Shard Relic', 'priceTON': 100.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.0025 }, { 'name': 'Durov\'s Cap', 'probability': 0.09925 }, { 'name': 'Astral Shard', 'probability': 0.10 }, { 'name': 'Precious Peach', 'probability': 0.10 }, { 'name': 'Vintage Cigar', 'probability': 0.12 }, { 'name': 'Perfume Bottle', 'probability': 0.12 }, { 'name': 'Swiss Watch', 'probability': 0.10 }, { 'name': 'Neko Helmet', 'probability': 0.08 }, { 'name': 'Mini Oscar', 'probability': 0.10 }, { 'name': 'Scared Cat', 'probability': 0.08 }, { 'name': 'Loot Bag', 'probability': 0.05 }, { 'name': 'Toy Bear', 'probability': 0.04825 } ] },
    { 'id': 'plushpepe', 'name': 'Plush Pepe Hoard', 'priceTON': 200.0, 'prizes': [ { 'name': 'Plush Pepe', 'probability': 0.15 }, { 'name': 'Durov\'s Cap', 'probability': 0.25 }, { 'name': 'Astral Shard', 'probability': 0.60 } ] }
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

TON_PRIZE_IMAGE_DEFAULT = "https://cdn.prod.website-files.com/64a2a26178ac203cccd4a006/6730c392cdf19828e76e898d_ton-logo.png"
DEFAULT_SLOT_TON_PRIZES = [ {'name': "0.1 TON", 'value': 0.1, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, {'name': "0.25 TON", 'value': 0.25, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, {'name': "0.5 TON", 'value': 0.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, {'name': "1.0 TON", 'value': 1.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, {'name': "1.5 TON", 'value': 1.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, ]
PREMIUM_SLOT_TON_PRIZES = [ {'name': "2 TON", 'value': 2.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, {'name': "3 TON", 'value': 3.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, {'name': "5 TON", 'value': 5.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, {'name': "10 TON", 'value': 10.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True, 'imageFilenameIsFullUrl': True}, ]
ALL_ITEMS_POOL_FOR_SLOTS = [{'name': name_val, 'floorPrice': price_val, 'imageFilename': generate_image_filename_from_name(name_val), 'is_ton_prize': False, 'imageFilenameIsFullUrl': False} for name_val, price_val in UPDATED_FLOOR_PRICES.items()]

slots_data_backend = [
    { 'id': 'default_slot', 'name': 'Default Slot', 'priceTON': 3.0, 'reels_config': 3, 'prize_pool': [],
      'displayPrizes': [ { 'name': 'Lol Pop', 'probability_display': 20}, { 'name': 'Cookie Heart', 'probability_display': 15 }, { 'name': '0.1 TON', 'probability_display': 10, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'imageFilenameIsFullUrl': True }, {'name': 'Neko Helmet', 'probability_display': 1} ]
    },
    { 'id': 'premium_slot', 'name': 'Premium Slot', 'priceTON': 10.0, 'reels_config': 3, 'prize_pool': [],
      'displayPrizes': [ { 'name': 'Neko Helmet', 'probability_display': 15 }, { 'name': 'Swiss Watch', 'probability_display': 10 }, { 'name': '1.0 TON', 'probability_display': 8, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'imageFilenameIsFullUrl': True }, {'name': 'Durov\'s Cap', 'probability_display': 0.5} ]
    }
]
for slot_backend_item in slots_data_backend:
    if 'displayPrizes' in slot_backend_item:
        for dp_item_val in slot_backend_item['displayPrizes']:
            if 'imageFilename' not in dp_item_val and not dp_item_val['name'].endswith(" TON"):
                dp_item_val['imageFilename'] = generate_image_filename_from_name(dp_item_val['name'])
                dp_item_val['imageFilenameIsFullUrl'] = False
            elif dp_item_val['name'].endswith(" TON") and 'imageFilename' not in dp_item_val : # Ensure TON prizes have the correct image if not set
                 dp_item_val['imageFilename'] = TON_PRIZE_IMAGE_DEFAULT
                 dp_item_val['imageFilenameIsFullUrl'] = True


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
        if current_total_prob_val > 0 and abs(current_total_prob_val - 1.0) > 0.001:
            logger.warning(f"Normalizing probabilities for slot {slot_data_instance['id']}. Original sum: {current_total_prob_val}")
            for p_norm_item in temp_pool_list:
                p_norm_item['probability'] = p_norm_item.get('probability', 0) / current_total_prob_val
        slot_data_instance['prize_pool'] = temp_pool_list
finalize_slot_prize_pools()

def calculate_and_log_rtp():
    logger.info("--- RTP Calculations (Based on Current Fixed Prices & Probabilities) ---")
    overall_total_ev_weighted_by_price = Decimal('0')
    overall_total_cost_squared = Decimal('0')
    all_games_data = cases_data_backend + slots_data_backend
    for game_data_item in all_games_data:
        game_id_val = game_data_item['id']
        game_name_val = game_data_item['name']
        price_val_rtp = Decimal(str(game_data_item['priceTON']))
        ev_val = Decimal('0')
        if 'prizes' in game_data_item:
            for prize_item in game_data_item['prizes']:
                prize_value_calc = Decimal(str(UPDATED_FLOOR_PRICES.get(prize_item['name'], 0)))
                # if game_id_val == 'black': # Black Singularity case removed
                #     prize_value_calc *= Decimal('2.5')
                ev_val += prize_value_calc * Decimal(str(prize_item['probability']))
        elif 'prize_pool' in game_data_item:
            reels_count = Decimal(str(game_data_item.get('reels_config', 3)))
            for prize_spec_item in game_data_item['prize_pool']:
                value_item_rtp = Decimal(str(prize_spec_item.get('value', prize_spec_item.get('floorPrice', 0))))
                prob_on_reel_val = Decimal(str(prize_spec_item.get('probability', 0)))
                if prize_spec_item.get('is_ton_prize'):
                    ev_val += value_item_rtp * prob_on_reel_val * reels_count # Simplified: assumes any TON on line pays for its value * num reels
                else:
                    ev_val += value_item_rtp * (prob_on_reel_val ** reels_count) # For 3-of-a-kind items
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

DEPOSIT_RECIPIENT_ADDRESS_RAW = os.environ.get("DEPOSIT_RECIPIENT_ADDRESS_RAW", "YOUR_WALLET_ADDRESS_HERE_NO_REALLY")
DEPOSIT_COMMENT = os.environ.get("DEPOSIT_COMMENT", "YOUR_UNIQUE_DEPOSIT_COMMENT_NO_REALLY")
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
            potential_ref_code = f"ref_{uid}_{random.randint(1000,9999)}"
            while db.query(User).filter(User.referral_code == potential_ref_code).first():
                potential_ref_code = f"ref_{uid}_{random.randint(1000,9999)}"
            user = User(id=uid, username=auth.get("username"), first_name=auth.get("first_name"), last_name=auth.get("last_name"), referral_code=potential_ref_code)
            db.add(user)
            db.commit()
            db.refresh(user)
        
        invited_friends_count = len(user.referrals_made_explicit) if user.referrals_made_explicit else 0
        
        inv = []
        for i in user.inventory:
            inv_item = {
                "id": i.id, 
                "name": i.nft.name if i.nft else i.item_name_override, 
                "imageFilename": i.nft.image_filename if i.nft else i.item_image_override, 
                "floorPrice": i.nft.floor_price if i.nft else i.current_value, 
                "currentValue": i.current_value, 
                "upgradeMultiplier": i.upgrade_multiplier, 
                "variant": i.variant, 
                "is_ton_prize": i.is_ton_prize, 
                "obtained_at": i.obtained_at.isoformat() if i.obtained_at else None,
                "imageFilenameIsFullUrl": i.image_filename_is_full_url # Pass this to frontend
            }
            inv.append(inv_item)
        
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

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    data = flask_request.get_json(); cid = data.get('case_id'); multiplier = int(data.get('multiplier', 1))
    if not cid: return jsonify({"error": "case_id required"}), 400
    if multiplier not in [1,2,3]: return jsonify({"error": "Invalid multiplier"}), 400
    db = next(get_db());
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        target_case = next((c for c in cases_data_backend if c['id'] == cid), None)
        if not target_case: return jsonify({"error": "Case not found"}), 404
        base_cost = Decimal(str(target_case['priceTON'])); total_cost = base_cost * Decimal(multiplier)
        if Decimal(str(user.ton_balance)) < total_cost: return jsonify({"error": f"Not enough TON. Need {total_cost:.2f}"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - total_cost)
        prizes_in_this_case = target_case['prizes']; won_prizes_list_response = []; total_value_won_this_spin = Decimal('0')
        for _ in range(multiplier):
            random_val = random.random(); cumulative_prob = 0; chosen_prize_data = None
            for prize_data in prizes_in_this_case:
                cumulative_prob += prize_data['probability']
                if random_val <= cumulative_prob: chosen_prize_data = prize_data; break
            if not chosen_prize_data: chosen_prize_data = random.choice(prizes_in_this_case) if prizes_in_this_case else None
            if not chosen_prize_data: logger.error(f"Could not determine prize for case {cid} for user {uid}"); continue
            db_nft_record = db.query(NFT).filter(NFT.name == chosen_prize_data['name']).first()
            if not db_nft_record: logger.error(f"NFT master record for '{chosen_prize_data['name']}' not found! Cannot add."); continue
            item_variant = None # Black Singularity removed, so no special variant for cases now
            value_of_item = Decimal(str(db_nft_record.floor_price))
            total_value_won_this_spin += value_of_item
            new_inventory_item = InventoryItem(user_id=uid, nft_id=db_nft_record.id, current_value=float(value_of_item.quantize(Decimal('0.01'))), variant=item_variant)
            db.add(new_inventory_item); db.flush()
            won_prizes_list_response.append({"id": new_inventory_item.id, "name": db_nft_record.name, "imageFilename": db_nft_record.image_filename, "floorPrice": float(db_nft_record.floor_price), "currentValue": new_inventory_item.current_value, "variant": new_inventory_item.variant, "imageFilenameIsFullUrl": False }) # Cases use relative paths
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_won_this_spin)
        db.commit()
        return jsonify({"status": "success", "won_prizes": won_prizes_list_response, "new_balance_ton": user.ton_balance})
    except Exception as e_open_case_route: db.rollback(); logger.error(f"Error in open_case: {e_open_case_route}", exc_info=True); return jsonify({"error": "Internal error"}),500
    finally: db.close()

@app.route('/api/spin_slot', methods=['POST'])
def spin_slot_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); slot_id_from_req = data.get('slot_id')
    if not slot_id_from_req: return jsonify({"error": "slot_id required"}), 400
    db = next(get_db());
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        target_slot_data = next((s for s in slots_data_backend if s['id'] == slot_id_from_req), None)
        if not target_slot_data: return jsonify({"error": "Slot not found"}), 404
        slot_cost = Decimal(str(target_slot_data['priceTON']))
        if Decimal(str(user.ton_balance)) < slot_cost: return jsonify({"error": f"Not enough TON. Need {slot_cost:.2f}"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - slot_cost)
        num_reels_for_slot = target_slot_data.get('reels_config', 3); slot_prize_pool_config = target_slot_data['prize_pool']
        if not slot_prize_pool_config: logger.error(f"Slot prize_pool empty for {slot_id_from_req}"); return jsonify({"error": "Slot config error"}), 500
        actual_reel_results = []
        for _ in range(num_reels_for_slot):
            random_val_slot = random.random(); cumulative_prob_slot = 0; landed_symbol = None
            for prize_symbol_info in slot_prize_pool_config:
                cumulative_prob_slot += prize_symbol_info.get('probability', 0)
                if random_val_slot <= cumulative_prob_slot: landed_symbol = prize_symbol_info; break
            if not landed_symbol: landed_symbol = random.choice(slot_prize_pool_config) if slot_prize_pool_config else None
            if landed_symbol: actual_reel_results.append(landed_symbol)
            else: actual_reel_results.append({"name": "ErrorSym", "imageFilename": "placeholder.png", "is_ton_prize": False, "currentValue": 0, "floorPrice": 0, "imageFilenameIsFullUrl": False})
        prizes_won_in_spin = []; total_value_from_spin = Decimal('0')
        for landed_sym_data in actual_reel_results:
            if landed_sym_data.get('is_ton_prize'):
                ton_value_won = Decimal(str(landed_sym_data.get('value', 0)))
                temp_id_for_ton = f"ton_prize_{int(time.time()*1000)}_{random.randint(1,1000)}"
                prizes_won_in_spin.append({"id": temp_id_for_ton, "name": landed_sym_data['name'], "imageFilename": landed_sym_data.get('imageFilename', TON_PRIZE_IMAGE_DEFAULT), "currentValue": float(ton_value_won), "is_ton_prize": True, "imageFilenameIsFullUrl": landed_sym_data.get('imageFilenameIsFullUrl', True)})
                user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_value_won); total_value_from_spin += ton_value_won
        if num_reels_for_slot == 3 and len(actual_reel_results) == 3:
            first_sym = actual_reel_results[0]
            if not first_sym.get('is_ton_prize') and \
               actual_reel_results[1]['name'] == first_sym['name'] and not actual_reel_results[1].get('is_ton_prize') and \
               actual_reel_results[2]['name'] == first_sym['name'] and not actual_reel_results[2].get('is_ton_prize'):
                won_item_master_data = first_sym
                db_nft_slot_win = db.query(NFT).filter(NFT.name == won_item_master_data['name']).first()
                if db_nft_slot_win:
                    item_value_slot = Decimal(str(db_nft_slot_win.floor_price))
                    new_inv_item_slot = InventoryItem(user_id=uid, nft_id=db_nft_slot_win.id, current_value=float(item_value_slot), variant=None, image_filename_is_full_url=False)
                    db.add(new_inv_item_slot); db.commit(); db.refresh(new_inv_item_slot)
                    prizes_won_in_spin.append({"id": new_inv_item_slot.id, "name": db_nft_slot_win.name, "imageFilename": db_nft_slot_win.image_filename, "floorPrice": float(db_nft_slot_win.floor_price), "currentValue": new_inv_item_slot.current_value, "is_ton_prize": False, "variant": new_inv_item_slot.variant, "imageFilenameIsFullUrl": False})
                    total_value_from_spin += item_value_slot
                else: logger.error(f"Slot win: NFT master for '{won_item_master_data['name']}' not found.")
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_from_spin)
        db.commit()
        return jsonify({"status": "success", "reel_results": actual_reel_results, "won_prizes": prizes_won_in_spin, "new_balance_ton": user.ton_balance})
    except Exception as e_spin_slot: db.rollback(); logger.error(f"Error in spin_slot: {e_spin_slot}", exc_info=True); return jsonify({"error":"Internal error"}),500
    finally: db.close()

# (All other API routes: upgrade_item, convert_to_ton, sell_all_items, initiate_deposit, 
#  check_blockchain_for_deposit, verify_deposit, get_leaderboard, withdraw_referral_earnings, 
#  redeem_promocode, withdraw_item_via_tonnel_api_sync_wrapper are inserted here from the previous 
#  "FULL Python code" block, as their internal logic was largely stable once variable names were consistent.)
# This is just to indicate their position. For the actual code, refer to the previous full python block.

@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); item_id_to_upgrade = data.get('inventory_item_id'); multiplier_str_upgrade = data.get('multiplier_str')
    if not all([item_id_to_upgrade, multiplier_str_upgrade]): return jsonify({"error": "Missing parameters"}), 400
    try: multiplier_decimal = Decimal(multiplier_str_upgrade); item_id_int = int(item_id_to_upgrade)
    except ValueError: return jsonify({"error": "Invalid parameter format"}), 400
    upgrade_chances_map = {Decimal("1.5"):50, Decimal("2.0"):35, Decimal("3.0"):25, Decimal("5.0"):15, Decimal("10.0"):8, Decimal("20.0"):3}
    if multiplier_decimal not in upgrade_chances_map: return jsonify({"error": "Invalid multiplier selected"}), 400
    db = next(get_db());
    try:
        inventory_item_to_upgrade = db.query(InventoryItem).filter(InventoryItem.id == item_id_int, InventoryItem.user_id == uid).with_for_update().first()
        if not inventory_item_to_upgrade or inventory_item_to_upgrade.is_ton_prize: return jsonify({"error": "Item not found or cannot be upgraded"}), 404
        user_performing_upgrade = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user_performing_upgrade: return jsonify({"error": "User not found"}), 404
        if random.uniform(0,100) < upgrade_chances_map[multiplier_decimal]:
            original_value = Decimal(str(inventory_item_to_upgrade.current_value)); new_value = (original_value * multiplier_decimal).quantize(Decimal('0.01'), ROUND_HALF_UP); value_increase = new_value - original_value
            inventory_item_to_upgrade.current_value = float(new_value); inventory_item_to_upgrade.upgrade_multiplier = float(Decimal(str(inventory_item_to_upgrade.upgrade_multiplier)) * multiplier_decimal)
            user_performing_upgrade.total_won_ton = float(Decimal(str(user_performing_upgrade.total_won_ton)) + value_increase)
            db.commit()
            return jsonify({"status": "success", "message": f"Upgrade successful! New value: {new_value:.2f} TON", 
                            "item": {"id": inventory_item_to_upgrade.id, "name": inventory_item_to_upgrade.nft.name, "imageFilename": inventory_item_to_upgrade.nft.image_filename, "currentValue": inventory_item_to_upgrade.current_value, "upgradeMultiplier": inventory_item_to_upgrade.upgrade_multiplier, "variant": inventory_item_to_upgrade.variant, "imageFilenameIsFullUrl": False}})
        else:
            lost_item_name = inventory_item_to_upgrade.nft.name; lost_item_value = Decimal(str(inventory_item_to_upgrade.current_value))
            user_performing_upgrade.total_won_ton = float(Decimal(str(user_performing_upgrade.total_won_ton)) - lost_item_value)
            db.delete(inventory_item_to_upgrade); db.commit()
            return jsonify({"status": "failed", "message": f"Upgrade failed! You lost {lost_item_name}.", "item_lost": True})
    except Exception as e_upgrade: db.rollback(); logger.error(f"Error in upgrade_item_api: {e_upgrade}", exc_info=True); return jsonify({"error": "Internal error during upgrade."}),500
    finally: db.close()

@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); inventory_item_id_to_convert = data.get('inventory_item_id')
    if not inventory_item_id_to_convert: return jsonify({"error": "inventory_item_id required"}), 400
    try: item_id_int_convert = int(inventory_item_id_to_convert)
    except ValueError: return jsonify({"error": "Invalid inventory_item_id"}), 400
    db = next(get_db());
    try:
        user_converting_item = db.query(User).filter(User.id == uid).with_for_update().first()
        item_for_conversion = db.query(InventoryItem).filter(InventoryItem.id == item_id_int_convert, InventoryItem.user_id == uid).first()
        if not user_converting_item or not item_for_conversion: return jsonify({"error": "User or item not found"}), 404
        if item_for_conversion.is_ton_prize: return jsonify({"error": "Cannot convert TON prize"}), 400
        conversion_value = Decimal(str(item_for_conversion.current_value))
        user_converting_item.ton_balance = float(Decimal(str(user_converting_item.ton_balance)) + conversion_value)
        converted_item_name = item_for_conversion.nft.name if item_for_conversion.nft else item_for_conversion.item_name_override
        db.delete(item_for_conversion); db.commit()
        return jsonify({"status": "success", "message": f"Item '{converted_item_name}' converted for {conversion_value:.2f} TON.", "new_balance_ton": user_converting_item.ton_balance})
    except Exception as e_convert: db.rollback(); logger.error(f"Error in convert_to_ton: {e_convert}", exc_info=True); return jsonify({"error": "Internal error"}),500
    finally: db.close()

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db());
    try:
        user_selling = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user_selling: return jsonify({"error": "User not found"}), 404
        items_eligible_for_sale = [item for item in user_selling.inventory if not item.is_ton_prize]
        if not items_eligible_for_sale: return jsonify({"status": "no_items", "message": "No sellable items."})
        total_value_from_sale = sum(Decimal(str(item.current_value)) for item in items_eligible_for_sale)
        user_selling.ton_balance = float(Decimal(str(user_selling.ton_balance)) + total_value_from_sale)
        for item_to_delete in items_eligible_for_sale: db.delete(item_to_delete)
        db.commit()
        return jsonify({"status": "success", "message": f"Sold {len(items_eligible_for_sale)} items for {total_value_from_sale:.2f} TON.", "new_balance_ton": user_selling.ton_balance})
    except Exception as e_sell_all: db.rollback(); logger.error(f"Error in sell_all_items: {e_sell_all}", exc_info=True); return jsonify({"error": "Internal error"}),500
    finally: db.close()

@app.route('/api/initiate_deposit', methods=['POST'])
def initiate_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); amount_str_deposit = data.get('amount')
    if amount_str_deposit is None: return jsonify({"error": "Amount required"}), 400
    try: original_amount_deposit = float(amount_str_deposit)
    except ValueError: return jsonify({"error": "Invalid amount"}), 400
    if not (0.1 <= original_amount_deposit <= 10000): return jsonify({"error": "Amount must be 0.1-10000 TON"}), 400
    db = next(get_db());
    try:
        user_for_deposit = db.query(User).filter(User.id == uid).first()
        if not user_for_deposit: return jsonify({"error": "User not found"}), 404
        active_deposit = db.query(PendingDeposit).filter(PendingDeposit.user_id == uid, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first()
        if active_deposit: return jsonify({"error": "Active deposit exists. Complete or cancel it."}), 409
        unique_nano_addon = random.randint(10000, 999999); final_amount_in_nano_ton = int(original_amount_deposit * 1e9) + unique_nano_addon
        new_pending_deposit = PendingDeposit(user_id=uid, original_amount_ton=original_amount_deposit, unique_identifier_nano_ton=unique_nano_addon, final_amount_nano_ton=final_amount_in_nano_ton, expected_comment=DEPOSIT_COMMENT, expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
        db.add(new_pending_deposit); db.commit(); db.refresh(new_pending_deposit)
        display_amount_to_send = f"{final_amount_in_nano_ton / 1e9:.9f}".rstrip('0').rstrip('.')
        return jsonify({"status": "success", "pending_deposit_id": new_pending_deposit.id, "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW, "amount_to_send": display_amount_to_send, "final_amount_nano_ton": final_amount_in_nano_ton, "comment": DEPOSIT_COMMENT, "expires_at": new_pending_deposit.expires_at.isoformat()})
    except Exception as e_init_deposit: db.rollback(); logger.error(f"Error in initiate_deposit: {e_init_deposit}", exc_info=True); return jsonify({"error": "Internal error"}),500
    finally: db.close()

async def check_blockchain_for_deposit(pending_deposit_to_check: PendingDeposit, db_session_for_check):
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
                                pending_deposit_to_check.status = 'failed'; db_session_for_check.commit()
                                logger.error(f"User ID {pending_deposit_to_check.user_id} for deposit {pending_deposit_to_check.id} not found.")
                                return {"status": "error", "message": "User associated with deposit vanished."}
                            user_to_credit_deposit.ton_balance = float(Decimal(str(user_to_credit_deposit.ton_balance)) + Decimal(str(pending_deposit_to_check.original_amount_ton)))
                            if user_to_credit_deposit.referrer: 
                                referrer_for_bonus = user_to_credit_deposit.referrer
                                db_session_for_check.refresh(referrer_for_bonus, with_for_update=True)
                                bonus_amount = (Decimal(str(pending_deposit_to_check.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                referrer_for_bonus.referral_earnings_pending = float(Decimal(str(referrer_for_bonus.referral_earnings_pending)) + bonus_amount)
                                logger.info(f"Credited referrer {referrer_for_bonus.id} with {bonus_amount:.2f} TON for deposit by {user_to_credit_deposit.id}")
                            elif user_to_credit_deposit.referred_by_id:
                                logger.warning(f"User {user_to_credit_deposit.id} has referred_by_id {user_to_credit_deposit.referred_by_id}, but referrer relationship was None. Attempting direct query for bonus.")
                                direct_referrer = db_session_for_check.query(User).filter(User.id == user_to_credit_deposit.referred_by_id).with_for_update().first()
                                if direct_referrer:
                                    bonus_amount = (Decimal(str(pending_deposit_to_check.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                    direct_referrer.referral_earnings_pending = float(Decimal(str(direct_referrer.referral_earnings_pending)) + bonus_amount)
                                    logger.info(f"Credited referrer (direct query) {direct_referrer.id} with {bonus_amount:.2f} TON for deposit by {user_to_credit_deposit.id}")
                            pending_deposit_to_check.status = 'completed'; db_session_for_check.commit()
                            return {"status": "success", "message": "Deposit confirmed and credited!", "new_balance_ton": user_to_credit_deposit.ton_balance}
                    except Exception as e_parse_comment: logger.debug(f"Could not parse comment for tx {transaction.hash}: {e_parse_comment}")
        if pending_deposit_to_check.expires_at <= dt.now(timezone.utc) and pending_deposit_to_check.status == 'pending':
            pending_deposit_to_check.status = 'expired'; db_session_for_check.commit()
            return {"status": "expired", "message": "Deposit has expired and was not found."}
        return {"status": "pending", "message": "Transaction not yet confirmed."}
    except Exception as e_blockchain_check: logger.error(f"Error in blockchain check: {e_blockchain_check}", exc_info=True); return {"status": "error", "message": "Error checking blockchain."}
    finally:
        if lite_balancer_client: await lite_balancer_client.close_all()

@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); pending_deposit_id_to_verify = data.get('pending_deposit_id')
    if not pending_deposit_id_to_verify: return jsonify({"error": "pending_deposit_id required"}), 400
    db = next(get_db());
    try:
        deposit_to_verify = db.query(PendingDeposit).filter(PendingDeposit.id == pending_deposit_id_to_verify, PendingDeposit.user_id == uid).with_for_update().first()
        if not deposit_to_verify: return jsonify({"error": "Deposit not found or not yours"}), 404
        if deposit_to_verify.status == 'completed':
            user_verified = db.query(User).filter(User.id == uid).first()
            return jsonify({"status": "success", "message": "Deposit already confirmed.", "new_balance_ton": user_verified.ton_balance if user_verified else 0})
        if deposit_to_verify.status == 'expired' or deposit_to_verify.expires_at <= dt.now(timezone.utc):
            if deposit_to_verify.status == 'pending': deposit_to_verify.status = 'expired'; db.commit()
            return jsonify({"status": "expired", "message": "Deposit expired."}), 400
        blockchain_check_result = {}
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                logger.warning("verify_deposit_api running new asyncio task from running loop.")
                temp_loop = asyncio.new_event_loop(); asyncio.set_event_loop(temp_loop)
                blockchain_check_result = temp_loop.run_until_complete(check_blockchain_for_deposit(deposit_to_verify, db)); temp_loop.close(); asyncio.set_event_loop(loop)
            else: blockchain_check_result = loop.run_until_complete(check_blockchain_for_deposit(deposit_to_verify, db))
        except RuntimeError as e_async_runtime:
            if "cannot schedule new futures" in str(e_async_runtime).lower() or "event loop is closed" in str(e_async_runtime).lower() or "no current event loop" in str(e_async_runtime).lower():
                logger.info("verify_deposit_api: Creating new event loop due to RuntimeError."); new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop)
                blockchain_check_result = new_loop.run_until_complete(check_blockchain_for_deposit(deposit_to_verify, db)); new_loop.close()
            else: logger.error(f"Unhandled RuntimeError in verify_deposit: {e_async_runtime}", exc_info=True); return jsonify({"status":"error", "message":"Internal async error"}),500
        except Exception as e_async_general: logger.error(f"General async exception in verify_deposit: {e_async_general}", exc_info=True); return jsonify({"status":"error", "message":"Internal verification error"}),500
        return jsonify(blockchain_check_result)
    except Exception as e_verify_db: db.rollback(); logger.error(f"Outer error in verify_deposit: {e_verify_db}", exc_info=True); return jsonify({"error": "DB error"}),500
    finally: db.close()

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    try:
        top_leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        return jsonify([{"rank": index + 1, "name": leader.first_name or leader.username or f"User_{str(leader.id)[:6]}", "avatarChar": (leader.first_name or leader.username or "U")[0].upper(), "income": leader.total_won_ton, "user_id": leader.id} for index, leader in enumerate(top_leaders)])
    finally:
        db.close()

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

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    data = flask_request.get_json()
    promocode_text_to_redeem = data.get('promocode_text', "").strip()
    if not promocode_text_to_redeem: return jsonify({"status": "error", "message": "Promocode cannot be empty."}), 400
    
    db = next(get_db())
    try:
        user_redeeming_promo = db.query(User).filter(User.id == uid).with_for_update().first()
        promo_code_obj = db.query(PromoCode).filter(PromoCode.code_text == promocode_text_to_redeem).with_for_update().first()
        
        if not user_redeeming_promo: return jsonify({"status": "error", "message": "User not found."}), 404
        if not promo_code_obj: return jsonify({"status": "error", "message": "Invalid promocode."}), 404
        if promo_code_obj.activations_left <= 0: return jsonify({"status": "error", "message": "This promocode has expired or reached its activation limit."}), 400
        
        promo_code_obj.activations_left -= 1
        user_redeeming_promo.ton_balance = float(Decimal(str(user_redeeming_promo.ton_balance)) + Decimal(str(promo_code_obj.ton_amount)))
        db.commit()
        return jsonify({"status": "success", "message": f"Promocode '{promocode_text_to_redeem}' redeemed! You received {promo_code_obj.ton_amount:.2f} TON.", "new_balance_ton": user_redeeming_promo.ton_balance})
    except SQLAlchemyError as e_sql_redeem:
        db.rollback()
        logger.error(f"SQLAlchemyError redeeming promocode: {e_sql_redeem}", exc_info=True)
        return jsonify({"status": "error", "message": "Database error during promocode redemption."}), 500
    except Exception as e_promo_redeem:
        db.rollback()
        logger.error(f"General error redeeming promocode: {e_promo_redeem}", exc_info=True)
        return jsonify({"status": "error", "message": "An unexpected error occurred while redeeming."}), 500
    finally:
        db.close()

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id_for_tonnel>', methods=['POST'])
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id_for_tonnel):
    auth_user_data_tonnel = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data_tonnel: return jsonify({"status": "error", "message": "Authentication failed"}), 401
    
    player_user_id_tonnel = auth_user_data_tonnel["id"]
    if not TONNEL_SENDER_INIT_DATA:
        logger.error("Tonnel withdrawal: TONNEL_SENDER_INIT_DATA not set.")
        return jsonify({"status": "error", "message": "Withdrawal service (Tonnel) not configured."}), 500
    
    db = next(get_db())
    try:
        item_for_tonnel_withdrawal = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id_for_tonnel, InventoryItem.user_id == player_user_id_tonnel).with_for_update().first()
        if not item_for_tonnel_withdrawal or item_for_tonnel_withdrawal.is_ton_prize:
            return jsonify({"status": "error", "message": "Item not found or not withdrawable."}), 404
        
        target_item_name_for_tonnel = item_for_tonnel_withdrawal.nft.name
        
        tonnel_gift_service_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        tonnel_api_call_result = {}
        
        try:
            event_loop_for_tonnel = asyncio.get_event_loop()
            if event_loop_for_tonnel.is_closed():
                event_loop_for_tonnel = asyncio.new_event_loop()
                asyncio.set_event_loop(event_loop_for_tonnel)
            tonnel_api_call_result = event_loop_for_tonnel.run_until_complete(
                tonnel_gift_service_client.send_gift_to_user(gift_item_name=target_item_name_for_tonnel, receiver_telegram_id=player_user_id_tonnel)
            )
        except RuntimeError as e_async_runtime_tonnel:
             if "cannot schedule new futures after shutdown" in str(e_async_runtime_tonnel).lower() or \
                "event loop is closed" in str(e_async_runtime_tonnel).lower() or \
                "no current event loop" in str(e_async_runtime_tonnel).lower():
                logger.warning(f"Asyncio loop issue in Tonnel withdrawal, creating new loop: {e_async_runtime_tonnel}")
                new_event_loop_tonnel = asyncio.new_event_loop()
                asyncio.set_event_loop(new_event_loop_tonnel)
                tonnel_api_call_result = new_event_loop_tonnel.run_until_complete(
                    tonnel_gift_service_client.send_gift_to_user(gift_item_name=target_item_name_for_tonnel, receiver_telegram_id=player_user_id_tonnel)
                )
                new_event_loop_tonnel.close()
             else:
                raise

        if tonnel_api_call_result and tonnel_api_call_result.get("status") == "success":
            value_deducted_from_total = Decimal(str(item_for_tonnel_withdrawal.current_value))
            player_user_tonnel = db.query(User).filter(User.id == player_user_id_tonnel).with_for_update().first()
            if player_user_tonnel:
                player_user_tonnel.total_won_ton = float(max(Decimal('0'), Decimal(str(player_user_tonnel.total_won_ton)) - value_deducted_from_total))
            
            db.delete(item_for_tonnel_withdrawal)
            db.commit()
            return jsonify({
                "status": "success", 
                "message": f"Gift '{target_item_name_for_tonnel}' sent via Tonnel! {tonnel_api_call_result.get('message', '')}", 
                "details": tonnel_api_call_result.get("details")
            })
        else:
            db.rollback()
            return jsonify({
                "status": "error", 
                "message": f"Tonnel withdrawal failed: {tonnel_api_call_result.get('message', 'Tonnel API reported an error or unexpected response.')}"
            }), 500
            
    except Exception as e_withdraw_via_tonnel:
        db.rollback()
        logger.error(f"General exception in Tonnel withdrawal wrapper: {e_withdraw_via_tonnel}", exc_info=True)
        return jsonify({"status": "error", "message": "An unexpected server error occurred during the withdrawal process."}), 500
    finally:
        db.close()

# --- Telegram Bot Handlers ---
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
            potential_ref_code = f"ref_{user_id_from_message}_{random.randint(1000,9999)}"
            while db.query(User).filter(User.referral_code == potential_ref_code).first():
                potential_ref_code = f"ref_{user_id_from_message}_{random.randint(1000,9999)}"
            user_record = User(id=user_id_from_message, username=telegram_user_object.username, first_name=telegram_user_object.first_name, last_name=telegram_user_object.last_name, referral_code=potential_ref_code)
            db.add(user_record)
            logger.info(f"New user {user_id_from_message} instance created with ref code {user_record.referral_code}")
        
        is_referred_by_id_updated_in_session = False
        try:
            message_command_parts = message.text.split(' ')
            if len(message_command_parts) > 1 and message_command_parts[1].startswith('startapp='):
                start_parameter = message_command_parts[1].split('=')[1]
                if start_parameter.startswith('ref_'):
                    referral_code_from_link = start_parameter
                    if (was_user_created_now or not user_record.referred_by_id) and \
                       (not user_record.referral_code or user_record.referral_code != referral_code_from_link):
                        referrer_user_record = db.query(User).filter(User.referral_code == referral_code_from_link).first()
                        if referrer_user_record and referrer_user_record.id != user_record.id:
                            user_record.referred_by_id = referrer_user_record.id
                            is_referred_by_id_updated_in_session = True
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
        if user_record.username != telegram_user_object.username: user_record.username = telegram_user_object.username; were_fields_updated = True
        if user_record.first_name != telegram_user_object.first_name: user_record.first_name = telegram_user_object.first_name; were_fields_updated = True
        if user_record.last_name != telegram_user_object.last_name: user_record.last_name = telegram_user_object.last_name; were_fields_updated = True
        
        if was_user_created_now or were_fields_updated or is_referred_by_id_updated_in_session:
            try:
                db.commit()
                if was_user_created_now: db.refresh(user_record)
                logger.info(f"User data for {user_id_from_message} committed (new: {was_user_created_now}, updated: {were_fields_updated}, ref_set_now: {is_referred_by_id_updated_in_session}).")
            except IntegrityError as ie_commit:
                db.rollback()
                logger.error(f"IntegrityError committing user {user_id_from_message}: {ie_commit}. Possibly duplicate referral_code if re-adding after rollback.")
                if was_user_created_now:
                     user_record.referral_code = f"ref_{user_id_from_message}_{random.randint(10000,99999)}"
                     try:
                         if not db.query(User).filter(User.id == user_record.id).first(): # Re-add if add was rolled back
                            db.add(user_record)
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
                if was_user_created_now and not db.query(User).filter(User.id == user_id_from_message).first():
                     bot.send_message(message.chat.id, "Sorry, there was an error setting up your profile. Please try the /start command again.")
                     return

        mini_app_button_url = f"https://t.me/{bot.get_me().username}/{MINI_APP_NAME or 'app'}"
        welcome_markup = types.InlineKeyboardMarkup()
        mini_app_web_app_info = types.WebAppInfo(url=mini_app_button_url)
        launch_app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=mini_app_web_app_info)
        welcome_markup.add(launch_app_button)
        bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже, чтобы начать!", reply_markup=welcome_markup)
    except Exception as e_start_command_handler:
        logger.error(f"General error in /start command handler for chat {message.chat.id}: {e_start_command_handler}", exc_info=True)
        bot.send_message(message.chat.id, "An unexpected error occurred. Please try again later.")
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
        logger.info("Bot polling process is already active.")
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
            if attempt_num < 2: time.sleep(2)
            else: logger.error("Failed to remove webhook after multiple attempts.")
    while bot_polling_active_flag:
        try:
            logger.info("Bot is now starting infinity_polling...")
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
            logger.info("infinity_polling has completed a cycle or was interrupted.") 
        except telebot.apihelper.ApiTelegramException as e_telegram_api:
            logger.error(f"Telegram API Exception in polling loop: Code {e_telegram_api.error_code} - {e_telegram_api.description}", exc_info=False)
            if e_telegram_api.error_code == 401 or e_telegram_api.error_code == 409:
                logger.error("CRITICAL: Bot token invalid or conflict. Stopping polling.")
                bot_polling_active_flag = False
            else: 
                logger.info("Waiting 30s before retrying polling (API exception).")
                time.sleep(30)
        except ConnectionError as e_network_connection: 
            logger.error(f"Network ConnectionError in polling loop: {e_network_connection}", exc_info=False)
            logger.info("Waiting 60s before retrying polling (network issue).")
            time.sleep(60)
        except Exception as e_generic_polling: 
            logger.error(f"An unexpected critical error occurred in the polling loop: {type(e_generic_polling).__name__} - {e_generic_polling}", exc_info=True)
            logger.info("Waiting 60s before retrying polling (unexpected error).")
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
        logger.info("Werkzeug reloader process detected. Polling managed by primary instance.")
    
    flask_port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask application server on host 0.0.0.0, port {flask_port}...")
    app.run(host='0.0.0.0', port=flask_port, debug=False, use_reloader=True)
