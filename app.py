import os
import logging
from flask import Flask, jsonify, request as flask_request, abort as flask_abort
from flask_cors import CORS
from dotenv import load_dotenv
import time
import random
import re
import hmac
import hashlib
from urllib.parse import unquote, parse_qs
from datetime import datetime as dt, timezone, timedelta
import json
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from curl_cffi.requests import AsyncSession, RequestsError
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad
from pytoniq import LiteBalancer
import asyncio
import math


load_dotenv()

# --- Configuration Constants ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24 # 24 hours for Telegram Mini App auth data
TONNEL_SENDER_INIT_DATA = os.environ.get("TONNEL_SENDER_INIT_DATA")
TONNEL_GIFT_SECRET = os.environ.get("TONNEL_GIFT_SECRET", "yowtfisthispieceofshitiiit")
API_KEY_SECRET = os.environ.get("API_KEY_SECRET", "YOUR_VERY_SECURE_RANDOM_API_KEY_FOR_FLOOR_UPDATES") # IMPORTANT: Change this and keep it secret!


DEPOSIT_RECIPIENT_ADDRESS_RAW = os.environ.get("DEPOSIT_WALLET_ADDRESS", "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR")
DEPOSIT_COMMENT = os.environ.get("DEPOSIT_COMMENT", "cpd7r07ud3s")
PENDING_DEPOSIT_EXPIRY_MINUTES = 30

RTP_TARGET = Decimal('0.85') # 85% Return to Player target

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backend_app.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if not API_KEY_SECRET or API_KEY_SECRET == "YOUR_VERY_SECURE_RANDOM_API_KEY_FOR_FLOOR_UPDATES":
    logger.warning("API_KEY_SECRET is not set or is using the default value! This is insecure for the floor price update endpoint.")

# --- SQLAlchemy Database Setup ---
engine = create_engine(DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Database Models (No changes here from your previous full code) ---
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
    floor_price = Column(Float, default=0.0, nullable=False) # This will be the live source of truth
    __table_args__ = (UniqueConstraint('name', name='uq_nft_name'),)

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    nft_id = Column(Integer, ForeignKey("nfts.id"), nullable=True)
    item_name_override = Column(String, nullable=True)
    item_image_override = Column(String, nullable=True)
    current_value = Column(Float, nullable=False) # Value at time of winning/upgrading
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
    activations_left = Column(Integer, nullable=False, default=0) # -1 for unlimited
    ton_amount = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

class UserPromoCodeRedemption(Base):
    __tablename__ = "user_promo_code_redemptions"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    promo_code_id = Column(Integer, ForeignKey("promo_codes.id", ondelete="CASCADE"), nullable=False)
    redeemed_at = Column(DateTime(timezone=True), server_default=func.now())
    user = relationship("User")
    promo_code = relationship("PromoCode")
    __table_args__ = (UniqueConstraint('user_id', 'promo_code_id', name='uq_user_promo_redemption'),)

Base.metadata.create_all(bind=engine)


# --- Tonnel Gift Sender (No changes here from your previous full code) ---
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

KISS_FROG_MODEL_STATIC_PERCENTAGES = { # Used by Tonnel Sender for model name matching
    "Brewtoad": 0.5,"Zodiak Croak": 0.5,"Rocky Hopper": 0.5,"Puddles": 0.5,"Lucifrog": 0.5,"Honeyhop": 0.5,
    "Count Croakula": 0.5,"Lilie Pond": 0.5,"Frogmaid": 0.5,"Happy Pepe": 0.5,"Melty Butter": 0.5,
    "Sweet Dream": 0.5,"Tree Frog": 0.5,"Lava Leap": 1.0,"Tesla Frog": 1.0,"Trixie": 1.0,"Pond Fairy": 1.0,
    "Icefrog": 1.0,"Hopberry": 1.5,"Boingo": 1.5,"Prince Ribbit": 1.5,"Toadstool": 1.5,"Cupid": 1.5,
    "Ms. Toad": 1.5,"Desert Frog": 1.5,"Silver": 2.0,"Bronze": 2.0,"Poison": 2.5,"Ramune": 2.5,
    "Lemon Drop": 2.5,"Minty Bloom": 2.5,"Void Hopper": 2.5,"Sarutoad": 2.5,"Duskhopper": 2.5,
    "Starry Night": 2.5,"Ectofrog": 2.5,"Ectobloom": 2.5,"Melon": 3.0,"Banana Pox": 3.0,"Frogtart": 3.0,
    "Sea Breeze": 4.0,"Sky Leaper": 4.0,"Toadberry": 4.0,"Peach": 4.0,"Lily Pond": 4.0,"Frogwave": 4.0,
    "Cranberry": 4.0,"Lemon Juice": 4.0,"Tide Pod": 4.0,"Brownie": 4.0,
}

class TonnelGiftSender:
    def __init__(self, sender_auth_data: str, gift_secret_passphrase: str):
        self.passphrase_secret = gift_secret_passphrase
        self.authdata = sender_auth_data
        self._session_instance: AsyncSession | None = None

    async def _get_session(self) -> AsyncSession:
        if self._session_instance is None:
            self._session_instance = AsyncSession(impersonate="chrome110")
        return self._session_instance

    async def _close_session_if_open(self):
        if self._session_instance:
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

            if method.upper() == "OPTIONS":
                if 200 <= response_obj.status_code < 300:
                    return {"status": "options_ok"}
                else:
                    err_text_options = await response_obj.text()
                    logger.error(f"Tonnel API OPTIONS {url} failed: {response_obj.status_code}. Resp: {err_text_options[:500]}")
                    response_obj.raise_for_status() # Will raise HTTPError
                    return {"status": "error", "message": f"OPTIONS failed: {response_obj.status_code}"} # Fallback

            response_obj.raise_for_status()

            if response_obj.status_code == 204: # No Content
                return None

            content_type = response_obj.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                try:
                    return response_obj.json()
                except json.JSONDecodeError as je_err_inner:
                    err_text_json_decode = await response_obj.text()
                    logger.error(f"Tonnel API JSONDecodeError for {method} {url}: {je_err_inner}. Body: {err_text_json_decode[:500]}")
                    return {"status": "error", "message": "Invalid JSON in response", "raw_text": err_text_json_decode[:500]}
            else:
                if is_initial_get: # For the very first GET, we don't care about JSON
                    return {"status": "get_ok_non_json"}
                else: # For other requests, non-JSON is usually an error or unexpected
                    responseText = await response_obj.text()
                    logger.warning(f"Tonnel API {method} {url} - Non-JSON (Type: {content_type}). Text: {responseText[:200]}")
                    return {"status": "error", "message": "Response not JSON", "content_type": content_type, "text_preview": responseText[:200]}

        except RequestsError as re_err: # Includes httpx.HTTPStatusError for 4xx/5xx
            logger.error(f"Tonnel API RequestsError ({method} {url}): {re_err}")
            # Try to get more details if it's an HTTPStatusError
            error_message = str(re_err)
            if hasattr(re_err, 'response') and re_err.response:
                try:
                    error_details = await re_err.response.text()
                    error_message += f" - Details: {error_details[:200]}"
                except: pass # Ignore if cannot get details
            raise Exception(f"Tonnel API Network/HTTP Error: {error_message}") from re_err
        except json.JSONDecodeError as je_err:
            logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err}")
            raise ValueError(f"Failed to decode JSON from {url}") from je_err
        except Exception as e_gen:
            logger.error(f"Tonnel API general request error ({method} {url}): {type(e_gen).__name__} - {e_gen}")
            raise


    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int):
        if not self.authdata:
            return {"status": "error", "message": "Tonnel sender not configured (missing auth data)."}

        try:
            await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)

            filter_dict = {
                "price": {"$exists": True},"refunded": {"$ne": True},"buyer": {"$exists": False},
                "export_at": {"$exists": True},"asset": "TON",
            }
            if gift_item_name in KISS_FROG_MODEL_STATIC_PERCENTAGES:
                static_percentage_val = KISS_FROG_MODEL_STATIC_PERCENTAGES[gift_item_name]
                formatted_percentage = f"{static_percentage_val:.1f}".rstrip('0').rstrip('.')
                filter_dict["gift_name"] = "Kissed Frog"
                filter_dict["model"] = f"{gift_item_name} ({formatted_percentage}%)"
            else:
                filter_dict["gift_name"] = gift_item_name
            filter_str = json.dumps(filter_dict)

            page_gifts_payload = {"filter":filter_str,"limit":10,"page":1,"sort":'{"price":1,"gift_id":-1}'}
            pg_headers_options = {"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type","Origin":"https://tonnel-gift.vercel.app","Referer":"https://tonnel-gift.vercel.app/"}
            pg_headers_post = {"Content-Type":"application/json","Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}

            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_options)
            gifts_found_response = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers_post, json_payload=page_gifts_payload)

            if not isinstance(gifts_found_response, list):
                return {"status":"error","message":f"Could not fetch gift list: {gifts_found_response.get('message','API error') if isinstance(gifts_found_response,dict) else 'Format error'}"}
            if not gifts_found_response:
                return {"status":"error","message":f"No '{gift_item_name}' gifts available on Tonnel marketplace."}
            
            low_gift = gifts_found_response[0]

            user_info_payload = {"authData":self.authdata,"user":receiver_telegram_id}
            ui_common_headers = {"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}
            ui_options_headers = {**ui_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            ui_post_headers = {**ui_common_headers,"Content-Type":"application/json"}
            
            await self._make_request(method="OPTIONS", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_options_headers)
            user_check_resp = await self._make_request(method="POST", url="https://gifts2.tonnel.network/api/userInfo", headers=ui_post_headers, json_payload=user_info_payload)

            if not isinstance(user_check_resp, dict) or user_check_resp.get("status") != "success":
                return {"status":"error","message":f"Tonnel user check failed: {user_check_resp.get('message','User error') if isinstance(user_check_resp,dict) else 'Unknown error'}"}

            encrypted_ts = encrypt_aes_cryptojs_compat(f"{int(time.time())}", self.passphrase_secret)
            buy_gift_url = f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}"
            buy_payload = {"anonymously":True,"asset":"TON","authData":self.authdata,"price":low_gift['price'],"receiver":receiver_telegram_id,"showPrice":False,"timestamp":encrypted_ts}
            buy_common_headers = {"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/","Host":"gifts.coffin.meme"}
            buy_options_headers = {**buy_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            buy_post_headers = {**buy_common_headers,"Content-Type":"application/json"}

            await self._make_request(method="OPTIONS", url=buy_gift_url, headers=buy_options_headers)
            purchase_resp = await self._make_request(method="POST", url=buy_gift_url, headers=buy_post_headers, json_payload=buy_payload, timeout=90)

            if isinstance(purchase_resp, dict) and purchase_resp.get("status") == "success":
                return {"status":"success","message":f"Gift '{gift_item_name}' sent!","details":purchase_resp}
            else:
                return {"status":"error","message":f"Tonnel transfer failed: {purchase_resp.get('message','Purchase error') if isinstance(purchase_resp,dict) else 'Unknown error'}"}

        except Exception as e:
            logger.error(f"Tonnel error sending gift '{gift_item_name}' to {receiver_telegram_id}: {type(e).__name__} - {e}", exc_info=True)
            return {"status":"error","message":f"Unexpected error during Tonnel withdrawal: {str(e)}"}
        finally:
            await self._close_session_if_open()


# --- Gift Data and Image Mapping ---
TON_PRIZE_IMAGE_DEFAULT = "https://case-bot.com/images/actions/ton.svg"
GIFT_NAME_TO_ID_MAP_PY = {
  "Santa Hat": "5983471780763796287","Signet Ring": "5936085638515261992","Precious Peach": "5933671725160989227","Plush Pepe": "5936013938331222567",
  "Spiced Wine": "5913442287462908725","Jelly Bunny": "5915502858152706668","Durov's Cap": "5915521180483191380","Perfume Bottle": "5913517067138499193",
  "Eternal Rose": "5882125812596999035","Berry Box": "5882252952218894938","Vintage Cigar": "5857140566201991735","Magic Potion": "5846226946928673709",
  "Kissed Frog": "5845776576658015084","Hex Pot": "5825801628657124140","Evil Eye": "5825480571261813595","Sharp Tongue": "5841689550203650524",
  "Trapped Heart": "5841391256135008713","Skull Flower": "5839038009193792264","Scared Cat": "5837059369300132790","Spy Agaric": "5821261908354794038",
  "Homemade Cake": "5783075783622787539","Genie Lamp": "5933531623327795414","Lunar Snake": "6028426950047957932","Party Sparkler": "6003643167683903930",
  "Jester Hat": "5933590374185435592","Witch Hat": "5821384757304362229","Hanging Star": "5915733223018594841","Love Candle": "5915550639663874519",
  "Cookie Heart": "6001538689543439169","Desk Calendar": "5782988952268964995","Jingle Bells": "6001473264306619020","Snow Mittens": "5980789805615678057",
  "Voodoo Doll": "5836780359634649414","Mad Pumpkin": "5841632504448025405","Hypno Lollipop": "5825895989088617224","B-Day Candle": "5782984811920491178",
  "Bunny Muffin": "5935936766358847989","Astral Shard": "5933629604416717361","Flying Broom": "5837063436634161765","Crystal Ball": "5841336413697606412",
  "Eternal Candle": "5821205665758053411","Swiss Watch": "5936043693864651359","Ginger Cookie": "5983484377902875708","Mini Oscar": "5879737836550226478",
  "Lol Pop": "5170594532177215681","Ion Gem": "5843762284240831056","Star Notepad": "5936017773737018241","Loot Bag": "5868659926187901653",
  "Love Potion": "5868348541058942091","Toy Bear": "5868220813026526561","Diamond Ring": "5868503709637411929","Sakura Flower": "5167939598143193218",
  "Sleigh Bell": "5981026247860290310","Top Hat": "5897593557492957738","Record Player": "5856973938650776169","Winter Wreath": "5983259145522906006",
  "Snow Globe": "5981132629905245483","Electric Skull": "5846192273657692751","Tama Gadget": "6023752243218481939","Candy Cane": "6003373314888696650",
  "Neko Helmet": "5933793770951673155","Jack-in-the-Box": "6005659564635063386","Easter Egg": "5773668482394620318",
  "Bonded Ring": "5870661333703197240", "Pet Snake": "6023917088358269866", "Snake Box": "6023679164349940429",
  "Xmas Stocking": "6003767644426076664", "Big Year": "6028283532500009446", "Holiday Drink": "6003735372041814769",
  "Gem Signet": "5859442703032386168", "Light Sword": "5897581235231785485"
}
INITIAL_NFT_FLOOR_PRICES_SEED = { # Used for initial DB population
    'Plush Pepe':1200.0,'Neko Helmet':15.0,'Sharp Tongue':17.0,"Durov's Cap":251.0,'Voodoo Doll':9.4,'Vintage Cigar':19.7,
    'Astral Shard':50.0,'Scared Cat':22.0,'Swiss Watch':18.6,'Perfume Bottle':38.3,'Precious Peach':162.0,
    'Toy Bear':16.3,'Genie Lamp':19.3,'Loot Bag':25.0,'Kissed Frog':14.8,'Electric Skull':10.9,'Diamond Ring':8.06,
    'Mini Oscar':40.5,'Party Sparkler':2.0,'Homemade Cake':2.0,'Cookie Heart':1.8,'Jack-in-the-box':2.0,'Skull Flower':3.4,
    'Lol Pop':1.4,'Hynpo Lollipop':1.4,'Desk Calendar':1.4,'B-Day Candle':1.4,'Record Player':4.0,'Jelly Bunny':3.6,
    'Tama Gadget':4.0,'Snow Globe':4.0,'Eternal Rose':11.0,'Love Potion':5.4,'Top Hat':6.0,
    'Berry Box':4.1, 'Bunny Muffin':4.0, 'Candy Cane':1.6, 'Crystal Ball':6.0, 'Easter Egg':1.8,
    'Eternal Candle':3.1, 'Evil Eye':4.2, 'Flying Broom':4.5, 'Ginger Cookie':2.7, 'Hanging Star':4.1,
    'Hex Pot':3.1, 'Ion Gem':44.0, 'Jester Hat':2.0, 'Jingle Bells':1.8, 'Love Candle':6.7,
    'Lunar Snake':1.5, 'Mad Pumpkin':6.2, 'Magic Potion':33.0, 'Pet Snake':3.2, 'Sakura Flower':4.1,
    'Santa Hat':2.0, 'Signet Ring':18.8, 'Sleigh Bell':6.0, 'Snow Mittens':2.9, 'Spiced Wine':2.2,
    'Spy Agaric':2.8, 'Star Notepad':2.8, 'Trapped Heart':6.0, 'Winter Wreath':2.0,
    # Kissed Frog Variants for initial seed
    "Happy Pepe":500.0,"Tree Frog":150.0,"Brewtoad":150.0,"Puddles":150.0,"Honeyhop":150.0,"Melty Butter":150.0,
    "Lucifrog":150.0,"Zodiak Croak":150.0,"Count Croakula":150.0,"Lilie Pond":150.0,"Sweet Dream":150.0,
    "Frogmaid":150.0,"Rocky Hopper":150.0,"Icefrog":45.0,"Lava Leap":45.0,"Toadstool":45.0,"Desert Frog":45.0,
    "Cupid":45.0,"Hopberry":45.0,"Ms. Toad":45.0,"Trixie":45.0,"Prince Ribbit":45.0,"Pond Fairy":45.0,
    "Boingo":45.0,"Tesla Frog":45.0,"Starry Night":30.0,"Silver":30.0,"Ectofrog":30.0,"Poison":30.0,
    "Minty Bloom":30.0,"Sarutoad":30.0,"Void Hopper":30.0,"Ramune":30.0,"Lemon Drop":30.0,"Ectobloom":30.0,
    "Duskhopper":30.0,"Bronze":30.0,"Lily Pond":19.0,"Toadberry":19.0,"Frogwave":19.0,"Melon":19.0,
    "Sky Leaper":19.0,"Frogtart":19.0,"Peach":19.0,"Sea Breeze":19.0,"Lemon Juice":19.0,"Cranberry":19.0,
    "Tide Pod":19.0,"Brownie":19.0,"Banana Pox":19.0
}
def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if "TON" in name_str.upper() and ("PRIZE" in name_str.upper() or name_str.replace('.', '', 1).replace(' TON', '').strip().replace(',', '').isdigit()):
        return TON_PRIZE_IMAGE_DEFAULT
    gift_id = GIFT_NAME_TO_ID_MAP_PY.get(name_str)
    if gift_id:
        return f"https://cdn.changes.tg/gifts/originals/{gift_id}/Original.png"
    # Check if it's a Kissed Frog variant (using the keys from initial seed as the source of truth for variants)
    if name_str in {k for k,v in INITIAL_NFT_FLOOR_PRICES_SEED.items() if "Frog" in k or "Toad" in k or "Pepe" in k or "Hop" in k or "Croak" in k}:
        # More specific check for Kissed Frog by looking for its patterns
        is_kissed_frog_variant = any(frog_kw in name_str for frog_kw in ["Frog", "Toad", "Pepe", "Hop", "Croak", "Butter", "Dream", "Puddles"])
        if name_str == "Kissed Frog": # The base gift
            pass # Will be handled by GIFT_NAME_TO_ID_MAP_PY
        elif is_kissed_frog_variant:
             return f"https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/{name_str.replace(' ', '%20')}.png"
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-Cigar.png"
    name_str_rep = name_str.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'
    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    filename = re.sub(r'-+', '-', cleaned)
    if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
        filename += '.png'
    return filename


# --- RTP Calculation Functions (No changes to their internal logic) ---
def calculate_rtp_probabilities(case_data, all_floor_prices):
    case_price = Decimal(str(case_data['priceTON']))
    target_ev = case_price * RTP_TARGET
    prizes = []
    for p_info in case_data['prizes']:
        prize_name = p_info['name']
        floor_price = Decimal(str(all_floor_prices.get(prize_name, 0)))
        image_filename = p_info.get('imageFilename', generate_image_filename_from_name(prize_name))
        is_ton_prize = p_info.get('is_ton_prize', False)
        prizes.append({'name': prize_name, 'probability': Decimal(str(p_info['probability'])), 'floor_price': floor_price, 'imageFilename': image_filename, 'is_ton_prize': is_ton_prize})
    if not prizes or all(p['floor_price'] == Decimal('0') for p in prizes):
        logger.warning(f"Case {case_data['id']} has no valuable prizes or no prizes. Normalizing probabilities without EV adjustment.")
        total_original_prob = sum(p['probability'] for p in prizes)
        normalized_prizes = []
        if total_original_prob > 0:
            for p in prizes:
                normalized_prizes.append({
                    'name': p['name'], 'probability': float((p['probability'] / total_original_prob).quantize(Decimal('1E-7'))),
                    'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
                })
        else:
            if prizes:
                equal_prob = Decimal('1.0') / len(prizes)
                for p in prizes:
                    normalized_prizes.append({
                        'name': p['name'], 'probability': float(equal_prob.quantize(Decimal('1E-7'))),
                        'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
                    })
        return normalized_prizes
    filler_prize_candidate = None; min_value = Decimal('inf')
    for p in prizes:
        if p['floor_price'] > 0:
            if p['floor_price'] < min_value: min_value = p['floor_price']; filler_prize_candidate = p
            elif p['floor_price'] == min_value and (filler_prize_candidate is None or p['probability'] > filler_prize_candidate['probability']): filler_prize_candidate = p
    if not filler_prize_candidate or filler_prize_candidate['floor_price'] == Decimal('0') or len(prizes) < 2:
        logger.warning(f"No suitable filler prize found for case {case_data['id']} or filler has 0 value or too few prizes. Falling back to proportional scaling.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
    filler_prize_idx = -1
    for i, p in enumerate(prizes):
        if p is filler_prize_candidate: filler_prize_idx = i; break
    if filler_prize_idx == -1:
        logger.error(f"Internal error: Filler prize not found in the prize list for case {case_data['id']}. Falling back to proportional scaling.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
    filler_prize = prizes[filler_prize_idx]
    sum_non_filler_ev = Decimal('0'); non_filler_total_initial_prob = Decimal('0')
    for p in prizes:
        if p is not filler_prize: sum_non_filler_ev += p['floor_price'] * p['probability']; non_filler_total_initial_prob += p['probability']
    remaining_ev_for_filler = target_ev - sum_non_filler_ev
    if filler_prize['floor_price'] == Decimal('0'):
        logger.error(f"Filler prize for case {case_data['id']} has 0 floor price during calculation. Using proportional scaling.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
    required_filler_prob = remaining_ev_for_filler / filler_prize['floor_price']
    if not (Decimal('0') <= required_filler_prob <= Decimal('1')):
        logger.warning(f"Required filler prob for {case_data['id']} out of bounds ({required_filler_prob}). Falling back to proportional scaling.")
        return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
    if non_filler_total_initial_prob > 0:
        scale_others_factor = (Decimal('1.0') - required_filler_prob) / non_filler_total_initial_prob
        if scale_others_factor < Decimal('0') or not math.isfinite(float(scale_others_factor)):
            logger.warning(f"Scale factor for non-filler items for {case_data['id']} is invalid ({scale_others_factor}). Falling back to proportional scaling.")
            return calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices)
        for p in prizes:
            if p is not filler_prize: p['probability'] *= scale_others_factor
    else: required_filler_prob = Decimal('1.0')
    filler_prize['probability'] = required_filler_prob
    current_sum_probs = sum(p['probability'] for p in prizes)
    if abs(current_sum_probs - Decimal('1.0')) > Decimal('1E-7'):
        diff = Decimal('1.0') - current_sum_probs
        if prizes: prizes[0]['probability'] += diff
    return [{'name': p['name'], 'probability': float(p['probability'].quantize(Decimal('1E-7'))),
             'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
            } for p in prizes]

def calculate_rtp_probabilities_proportional_fallback(case_data, all_floor_prices):
    case_price = Decimal(str(case_data['priceTON']))
    target_ev = case_price * RTP_TARGET
    prizes = []
    for p_info in case_data['prizes']:
        prize_name = p_info['name']
        floor_price = Decimal(str(all_floor_prices.get(prize_name, 0)))
        image_filename = p_info.get('imageFilename', generate_image_filename_from_name(prize_name))
        is_ton_prize = p_info.get('is_ton_prize', False)
        prizes.append({'name': prize_name, 'probability': Decimal(str(p_info['probability'])), 'floor_price': floor_price, 'imageFilename': image_filename, 'is_ton_prize': is_ton_prize})
    current_total_ev = sum(p['floor_price'] * p['probability'] for p in prizes)
    if current_total_ev == Decimal('0'):
        logger.warning(f"Proportional fallback for {case_data['id']}: Current total EV is zero. Normalizing probabilities without EV adjustment.")
        total_original_prob = sum(p['probability'] for p in prizes)
        normalized_prizes = []
        if total_original_prob > 0:
            for p in prizes:
                normalized_prizes.append({
                    'name': p['name'], 'probability': float((p['probability'] / total_original_prob).quantize(Decimal('1E-7'))),
                    'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
                })
        else:
            if prizes:
                equal_prob = Decimal('1.0') / len(prizes)
                for p in prizes:
                    normalized_prizes.append({
                        'name': p['name'], 'probability': float(equal_prob.quantize(Decimal('1E-7'))),
                        'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
                    })
        return normalized_prizes
    scale_factor = target_ev / current_total_ev
    for p in prizes: p['probability'] = p['probability'] * scale_factor
    total_prob_after_scaling = sum(p['probability'] for p in prizes)
    if total_prob_after_scaling == Decimal('0'):
        logger.error(f"Proportional fallback for {case_data['id']}: Total probability after scaling is zero. Cannot normalize.")
        return []
    for p in prizes: p['probability'] = p['probability'] / total_prob_after_scaling
    final_total_prob = sum(p['probability'] for p in prizes)
    if abs(final_total_prob - Decimal('1.0')) > Decimal('1E-7'):
        diff = Decimal('1.0') - final_total_prob
        if prizes: prizes[0]['probability'] += diff
    return [{'name': p['name'], 'probability': float(p['probability'].quantize(Decimal('1E-7'))),
             'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
            } for p in prizes]

def calculate_rtp_probabilities_for_slots(slot_data, all_floor_prices):
    slot_price = Decimal(str(slot_data['priceTON']))
    target_ev = slot_price * RTP_TARGET
    num_reels = Decimal(str(slot_data.get('reels_config', 3)))
    prizes = []
    for p_info in slot_data['prize_pool']:
        prize_name = p_info['name']
        value_source = p_info.get('value', all_floor_prices.get(prize_name, 0)) # Use `value` for TON, else lookup floor price
        floor_price = Decimal(str(value_source))
        image_filename = p_info.get('imageFilename', generate_image_filename_from_name(prize_name))
        is_ton_prize = p_info.get('is_ton_prize', False)
        prizes.append({'name': prize_name, 'probability': Decimal(str(p_info['probability'])), 'floor_price': floor_price, 'imageFilename': image_filename, 'is_ton_prize': is_ton_prize})
    current_total_ev = Decimal('0')
    for p in prizes:
        if p['is_ton_prize']: current_total_ev += p['probability'] * p['floor_price'] * num_reels
        else: current_total_ev += (p['probability'] ** num_reels) * p['floor_price']
    if current_total_ev == Decimal('0'):
        logger.warning(f"Slot {slot_data['id']}: Current total EV is zero. Normalizing probabilities without EV adjustment.")
        total_original_prob = sum(p['probability'] for p in prizes)
        normalized_prizes = []
        if total_original_prob > 0:
            for p in prizes:
                normalized_prizes.append({
                    'name': p['name'], 'probability': float((p['probability'] / total_original_prob).quantize(Decimal('1E-7'))),
                    'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
                })
        else:
            if prizes:
                equal_prob = Decimal('1.0') / len(prizes)
                for p in prizes:
                    normalized_prizes.append({
                        'name': p['name'], 'probability': float(equal_prob.quantize(Decimal('1E-7'))),
                        'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
                    })
        return normalized_prizes
    scale_factor = target_ev / current_total_ev
    for p in prizes: p['probability'] *= scale_factor
    total_prob_after_scaling = sum(p['probability'] for p in prizes)
    if total_prob_after_scaling == Decimal('0'):
        logger.error(f"Slot {slot_data['id']}: Total probability after scaling is zero. Cannot normalize.")
        return []
    for p in prizes: p['probability'] /= total_prob_after_scaling
    final_total_prob = sum(p['probability'] for p in prizes)
    if abs(final_total_prob - Decimal('1.0')) > Decimal('1E-7'):
        diff = Decimal('1.0') - final_total_prob
        if prizes: prizes[0]['probability'] += diff
    return [{'name': p['name'], 'probability': float(p['probability'].quantize(Decimal('1E-7'))),
             'floor_price': float(p['floor_price']), 'imageFilename': p['imageFilename'], 'is_ton_prize': p['is_ton_prize']
            } for p in prizes]

# --- Game Data Templates (Raw probabilities, no floor prices here) ---
cases_data_backend_raw_templates = [
    {'id':'lolpop','name':'Lol Pop Stash','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Lol-Pop.jpg','priceTON':2.0,'prizes':[
        {'name':'Plush Pepe','probability':0.00005}, {'name':'Neko Helmet','probability':0.0015},
        {'name':'Party Sparkler','probability':0.08}, {'name':'Homemade Cake','probability':0.08},
        {'name':'Cookie Heart','probability':0.08}, {'name':'Jack-in-the-box','probability':0.08},
        {'name':'Skull Flower','probability':0.035}, {'name':'Lol Pop','probability':0.15},
        {'name':'Hynpo Lollipop','probability':0.15}, {'name':'Desk Calendar','probability':0.05},
        {'name':'B-Day Candle','probability':0.05}, {'name':'Candy Cane','probability':0.05},
        {'name':'Easter Egg','probability':0.05}, {'name':'Jingle Bells','probability':0.05},
        {'name':'Lunar Snake','probability':0.05}, {'name':'Santa Hat','probability':0.05},
        {'name':'Jester Hat','probability':0.05}, {'name':'Spiced Wine','probability':0.05},
        {'name':'Winter Wreath','probability':0.05}
    ]},
    {'id':'recordplayer','name':'Record Player Vault','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Record-Player.jpg','priceTON':6.0,'prizes':[
        {'name':'Plush Pepe','probability':0.00015},{'name':'Record Player','probability':0.15},
        {'name':'Lol Pop','probability':0.10},{'name':'Hynpo Lollipop','probability':0.10},
        {'name':'Party Sparkler','probability':0.10},{'name':'Skull Flower','probability':0.08},
        {'name':'Jelly Bunny','probability':0.08},{'name':'Tama Gadget','probability':0.07},
        {'name':'Snow Globe','probability':0.06}, {'name':'Bunny Muffin','probability':0.03},
        {'name':'Berry Box','probability':0.03}, {'name':'Crystal Ball','probability':0.03},
        {'name':'Eternal Candle','probability':0.03}, {'name':'Evil Eye','probability':0.03},
        {'name':'Flying Broom','probability':0.03}, {'name':'Hex Pot','probability':0.03},
        {'name':'Pet Snake','probability':0.03}, {'name':'Snow Mittens','probability':0.03},
        {'name':'Spy Agaric','probability':0.03}, {'name':'Star Notepad','probability':0.03},
        {'name':'Ginger Cookie','probability':0.03},
    ]},
    { 'id': 'girls_collection', 'name': 'Girl\'s Collection', 'imageFilename': 'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/girls.jpg','priceTON': 8.0, 'prizes': [
            {'name': 'Loot Bag', 'probability': 0.1}, {'name': 'Neko Helmet', 'probability': 0.1},
            {'name': 'Genie Lamp', 'probability': 0.1}, {'name': 'Eternal Rose', 'probability': 0.1},
            {'name': 'Sharp Tongue', 'probability': 0.1}, {'name': 'Toy Bear', 'probability': 0.1},
            {'name': 'Star Notepad', 'probability': 0.1}, {'name': 'Bunny Muffin', 'probability': 0.1},
            {'name': 'Berry Box', 'probability': 0.1}, {'name': 'Sakura Flower', 'probability': 0.05},
            {'name': 'Cookie Heart', 'probability': 0.05}
        ]
    },
    { 'id': 'mens_collection', 'name': 'Men\'s Collection', 'imageFilename': 'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/men.jpg', 'priceTON': 8.0, 'prizes': [
            {'name': 'Durov\'s Cap', 'probability': 0.1}, {'name': 'Signet Ring', 'probability': 0.1},
            {'name': 'Swiss Watch', 'probability': 0.1}, {'name': 'Vintage Cigar', 'probability': 0.1},
            {'name': 'Mini Oscar', 'probability': 0.1}, {'name': 'Perfume Bottle', 'probability': 0.1},
            {'name': 'Scared Cat', 'probability': 0.1}, {'name': 'Record Player', 'probability': 0.1},
            {'name': 'Top Hat', 'probability': 0.1}, {'name': 'Spiced Wine', 'probability': 0.1}
        ]
    },
    {'id':'swisswatch','name':'Swiss Watch Box','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Swiss-Watch.jpg','priceTON':10.0,'prizes':[
        {'name':'Plush Pepe','probability':0.0002},{'name':'Swiss Watch','probability':0.032},
        {'name':'Neko Helmet','probability':0.045},{'name':'Eternal Rose','probability':0.06},
        {'name':'Electric Skull','probability':0.08},{'name':'Diamond Ring','probability':0.1},
        {'name':'Record Player','probability':0.12},{'name':'Love Potion','probability':0.12},
        {'name':'Top Hat','probability':0.12},{'name':'Voodoo Doll','probability':0.15},
        {'name':'Love Candle','probability':0.04}, {'name':'Signet Ring','probability':0.04},
        {'name':'Sleigh Bell','probability':0.04}, {'name':'Trapped Heart','probability':0.04},
        {'name':'Mad Pumpkin','probability':0.04}
    ]},
    {'id':'kissedfrog','name':'Kissed Frog Pond','priceTON':20.0,'imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Kissed-Frog.jpg','prizes':[
        {'name':'Happy Pepe','probability':0.00010},{'name':'Tree Frog','probability':0.00050},{'name':'Brewtoad','probability':0.00050},
        {'name':'Puddles','probability':0.00050},{'name':'Honeyhop','probability':0.00050},{'name':'Melty Butter','probability':0.00050},
        {'name':'Lucifrog','probability':0.00050},{'name':'Zodiak Croak','probability':0.00050},{'name':'Count Croakula','probability':0.00050},
        {'name':'Lilie Pond','probability':0.00050},{'name':'Sweet Dream','probability':0.00050},{'name':'Frogmaid','probability':0.00050},
        {'name':'Rocky Hopper','probability':0.00050},{'name':'Icefrog','probability':0.0020},{'name':'Lava Leap','probability':0.0020},
        {'name':'Toadstool','probability':0.0020},{'name':'Desert Frog','probability':0.0020},{'name':'Cupid','probability':0.0020},
        {'name':'Hopberry','probability':0.0020},{'name':'Ms. Toad','probability':0.0020},{'name':'Trixie','probability':0.0020},
        {'name':'Prince Ribbit','probability':0.0020},{'name':'Pond Fairy','probability':0.0020},{'name':'Boingo','probability':0.0020},
        {'name':'Tesla Frog','probability':0.0020},{'name':'Starry Night','probability':0.0070},{'name':'Silver','probability':0.0070},
        {'name':'Ectofrog','probability':0.0070},{'name':'Poison','probability':0.0070},{'name':'Minty Bloom','probability':0.0070},
        {'name':'Sarutoad','probability':0.0070},{'name':'Void Hopper','probability':0.0070},{'name':'Ramune','probability':0.0070},
        {'name':'Lemon Drop','probability':0.0070},{'name':'Ectobloom','probability':0.0070},{'name':'Duskhopper','probability':0.0070},
        {'name':'Bronze','probability':0.0070},{'name':'Lily Pond','probability':0.04028},{'name':'Toadberry','probability':0.04028},
        {'name':'Frogwave','probability':0.04028},{'name':'Melon','probability':0.04028},{'name':'Sky Leaper','probability':0.04028},
        {'name':'Frogtart','probability':0.04028},{'name':'Peach','probability':0.04028},{'name':'Sea Breeze','probability':0.04028},
        {'name':'Lemon Juice','probability':0.04028},{'name':'Cranberry','probability':0.04028},{'name':'Tide Pod','probability':0.04028},
        {'name':'Brownie','probability':0.04028},{'name':'Banana Pox','probability':0.04024},{'name':'Desk Calendar','probability':0.1} # Added small prob for consolation
    ]},
    {'id':'perfumebottle','name':'Perfume Chest','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Perfume-Bottle.jpg','priceTON': 20.0,'prizes':[
        {'name':'Plush Pepe','probability':0.0004},{'name':'Perfume Bottle','probability':0.02},
        {'name':'Sharp Tongue','probability':0.035},{'name':'Loot Bag','probability':0.05},
        {'name':'Swiss Watch','probability':0.06},{'name':'Neko Helmet','probability':0.08},
        {'name':'Genie Lamp','probability':0.11},{'name':'Kissed Frog','probability':0.15},
        {'name':'Electric Skull','probability':0.2},{'name':'Diamond Ring','probability':0.2},
        {'name':'Magic Potion','probability':0.05}, {'name':'Ion Gem','probability':0.04}
    ]},
    {'id':'vintagecigar','name':'Vintage Cigar Safe','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Vintage-Cigar.jpg','priceTON':40.0,'prizes':[
        {'name':'Plush Pepe','probability':0.0008},{'name':'Perfume Bottle','probability':0.025},
        {'name':'Vintage Cigar','probability':0.03},{'name':'Swiss Watch','probability':0.04},
        {'name':'Neko Helmet','probability':0.06},{'name':'Sharp Tongue','probability':0.08},
        {'name':'Genie Lamp','probability':0.1},{'name':'Mini Oscar','probability':0.07},
        {'name':'Scared Cat','probability':0.2},{'name':'Toy Bear','probability':0.3942},
        {'name':'Precious Peach','probability':0.02}
    ]},
    {'id':'astralshard','name':'Astral Shard Relic','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Astral-Shard.jpg','priceTON':100.0,'prizes':[
        {'name':'Plush Pepe','probability':0.0015},{'name':'Durov\'s Cap','probability':0.01},
        {'name':'Astral Shard','probability':0.025},{'name':'Precious Peach','probability':0.05},
        {'name':'Vintage Cigar','probability':0.04},{'name':'Perfume Bottle','probability':0.05},
        {'name':'Swiss Watch','probability':0.07},{'name':'Neko Helmet','probability':0.09},
        {'name':'Mini Oscar','probability':0.06},{'name':'Scared Cat','probability':0.15},
        {'name':'Loot Bag','probability':0.2},{'name':'Toy Bear','probability':0.2},
        {'name':'Ion Gem','probability':0.03}, {'name':'Magic Potion','probability':0.03}
    ]},
    {'id':'plushpepe','name':'Plush Pepe Hoard','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Plush-Pepe.jpg','priceTON': 200.0,'prizes':[
        {'name':'Plush Pepe','probability':0.045},{'name':'Durov\'s Cap','probability':0.2},
        {'name':'Astral Shard','probability':0.4},{'name':'Precious Peach','probability':0.3}
    ]}
]
slots_data_backend_raw_templates = [
    { 'id': 'default_slot', 'name': 'Default Slot', 'priceTON': 3.0, 'reels_config': 3, 'prize_pool': [
        {'name': "0.1 TON", 'value': 0.1, 'is_ton_prize': True, 'probability': 0.1},
        {'name': "0.25 TON", 'value': 0.25, 'is_ton_prize': True, 'probability': 0.08},
        {'name': "0.5 TON", 'value': 0.5, 'is_ton_prize': True, 'probability': 0.05},
        {'name': 'Desk Calendar', 'probability': 0.03, 'is_ton_prize': False}, 
        {'name': 'Lol Pop', 'probability': 0.03, 'is_ton_prize': False},
        {'name': 'Party Sparkler', 'probability': 0.02, 'is_ton_prize': False}, 
        {'name': 'Cookie Heart', 'probability': 0.02, 'is_ton_prize': False},
        {'name': 'Jack-in-the-box', 'probability': 0.01, 'is_ton_prize': False}, 
        {'name': 'Skull Flower', 'probability': 0.01, 'is_ton_prize': False}
    ]},
    { 'id': 'premium_slot', 'name': 'Premium Slot', 'priceTON': 10.0, 'reels_config': 3, 'prize_pool': [
        {'name': "2 TON", 'value': 2.0, 'is_ton_prize': True, 'probability': 0.08},
        {'name': "3 TON", 'value': 3.0, 'is_ton_prize': True, 'probability': 0.05},
        {'name': "5 TON", 'value': 5.0, 'is_ton_prize': True, 'probability': 0.03},
        {'name': 'Plush Pepe', 'probability': 0.001, 'is_ton_prize': False}, 
        {'name': 'Durov\'s Cap', 'probability': 0.002, 'is_ton_prize': False},
        {'name': 'Astral Shard', 'probability': 0.003, 'is_ton_prize': False}, 
        {'name': 'Precious Peach', 'probability': 0.004, 'is_ton_prize': False},
        {'name': 'Vintage Cigar', 'probability': 0.005, 'is_ton_prize': False}
    ]}
]

# Global variables for storing processed data for frontend initial load
cases_data_backend = []
slots_data_backend = []

# --- Helper to get live floor prices ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_all_live_floor_prices_from_db():
    db = next(get_db())
    try:
        nfts = db.query(NFT).all()
        return {nft.name: float(nft.floor_price) for nft in nfts}
    except Exception as e:
        logger.error(f"Error fetching live floor prices from DB: {e}", exc_info=True)
        return {} # Return empty if error, frontend should handle this
    finally:
        db.close()

# --- Initial Data Population and Setup ---
def populate_initial_data():
    db = SessionLocal()
    try:
        for nft_name, floor_price in INITIAL_NFT_FLOOR_PRICES_SEED.items():
            nft_exists = db.query(NFT).filter(NFT.name == nft_name).first()
            img_filename_or_url = generate_image_filename_from_name(nft_name)
            if not nft_exists:
                db.add(NFT(name=nft_name, image_filename=img_filename_or_url, floor_price=floor_price))
            elif nft_exists.floor_price != floor_price or nft_exists.image_filename != img_filename_or_url:
                if nft_exists.floor_price != floor_price:
                    logger.info(f"Updating initial seed floor price for {nft_name}: {nft_exists.floor_price} -> {floor_price}")
                    nft_exists.floor_price = floor_price
                if nft_exists.image_filename != img_filename_or_url:
                    logger.info(f"Updating initial seed image for {nft_name}: {nft_exists.image_filename} -> {img_filename_or_url}")
                    nft_exists.image_filename = img_filename_or_url
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error populating initial NFT data: {e}", exc_info=True)
    finally:
        db.close()

def initial_setup_and_logging():
    populate_initial_data()
    db = SessionLocal()
    try:
        if not db.query(PromoCode).filter(PromoCode.code_text == 'Grachev').first():
            db.add(PromoCode(code_text='Grachev', activations_left=10, ton_amount=100.0))
            db.commit()
            logger.info("Seeded 'Grachev' promocode.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error seeding Grachev promocode: {e}", exc_info=True)
    finally:
        db.close()
    
    current_db_floor_prices_initial_load = get_all_live_floor_prices_from_db()
    if not current_db_floor_prices_initial_load:
        logger.error("CRITICAL: Could not load floor prices from DB for initial setup. Frontend data might be incomplete.")
        # Potentially load from INITIAL_NFT_FLOOR_PRICES_SEED as a fallback for initial frontend data
        current_db_floor_prices_initial_load = INITIAL_NFT_FLOOR_PRICES_SEED
        logger.warning("Using INITIAL_NFT_FLOOR_PRICES_SEED as fallback for initial frontend data due to DB load failure.")


    global cases_data_backend, slots_data_backend
    cases_data_backend = []
    for case_template in cases_data_backend_raw_templates:
        processed_case = {**case_template}
        try:
            processed_case['prizes'] = calculate_rtp_probabilities(case_template, current_db_floor_prices_initial_load)
            cases_data_backend.append(processed_case)
        except Exception as e:
            logger.error(f"Failed to process case template '{case_template.get('id')}' for initial RTP: {e}", exc_info=True)
    
    slots_data_backend = []
    for slot_template in slots_data_backend_raw_templates:
        processed_slot = {**slot_template}
        try:
            processed_slot['prize_pool'] = calculate_rtp_probabilities_for_slots(slot_template, current_db_floor_prices_initial_load)
            slots_data_backend.append(processed_slot)
        except Exception as e:
            logger.error(f"Failed to process slot template '{slot_template.get('id')}' for initial RTP: {e}", exc_info=True)

    calculate_and_log_rtp() # Logs RTP based on initially processed data

# --- Telegram Mini App InitData Validation (No changes from your previous full code) ---
def validate_init_data(init_data_str: str, bot_token_for_validation: str) -> dict | None:
    logger.debug(f"Attempting to validate initData: {init_data_str[:200]}...")
    try:
        if not init_data_str:
            logger.warning("validate_init_data: init_data_str is empty or None.")
            return None
        parsed_data = dict(parse_qs(init_data_str))
        for key, value_list in parsed_data.items():
            if value_list: parsed_data[key] = value_list[0]
            else: logger.warning(f"validate_init_data: Empty value list for key: {key}"); return None
        required_keys = ['hash', 'user', 'auth_date']
        missing_keys = [k for k in required_keys if k not in parsed_data]
        if missing_keys:
            logger.warning(f"validate_init_data: Missing keys in parsed_data: {missing_keys}. Parsed: {list(parsed_data.keys())}")
            return None
        hash_received = parsed_data.pop('hash')
        auth_date_ts = int(parsed_data['auth_date'])
        current_ts = int(dt.now(timezone.utc).timestamp())
        if (current_ts - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS:
            logger.warning(f"validate_init_data: auth_date expired. auth_date_ts: {auth_date_ts}, current_ts: {current_ts}, diff: {current_ts - auth_date_ts}s, max_age: {AUTH_DATE_MAX_AGE_SECONDS}s")
            return None
        data_check_string_parts = []
        for k in sorted(parsed_data.keys()):
            if k == 'user': data_check_string_parts.append(f"{k}={unquote(parsed_data[k])}")
            else: data_check_string_parts.append(f"{k}={parsed_data[k]}")
        data_check_string = "\n".join(data_check_string_parts)
        secret_key = hmac.new("WebAppData".encode(), bot_token_for_validation.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash_hex == hash_received:
            user_info_str_unquoted = unquote(parsed_data['user'])
            try: user_info_dict = json.loads(user_info_str_unquoted)
            except json.JSONDecodeError as je: logger.error(f"validate_init_data: Failed to parse user JSON: {user_info_str_unquoted}. Error: {je}"); return None
            if 'id' not in user_info_dict: logger.warning(f"validate_init_data: 'id' not found in user_info_dict. User data: {user_info_dict}"); return None
            user_info_dict['id'] = int(user_info_dict['id'])
            logger.info(f"validate_init_data: Hash matched for user ID: {user_info_dict.get('id')}. Auth successful.")
            return user_info_dict
        else:
            logger.warning(f"validate_init_data: Hash mismatch.")
            logger.debug(f"Received Hash: {hash_received}; Calculated Hash: {calculated_hash_hex}; Data Check String: {data_check_string[:500]}")
            return None
    except Exception as e_validate:
        logger.error(f"validate_init_data: General exception during initData validation: {e_validate}", exc_info=True)
        return None

# --- Flask App Setup ---
app = Flask(__name__)
PROD_ORIGIN = "https://vasiliy-katsyka.github.io"
NULL_ORIGIN = "null"
LOCAL_DEV_ORIGINS = ["http://localhost:5500","http://127.0.0.1:5500","http://localhost:8000","http://127.0.0.1:8000",]
final_allowed_origins = list(set([PROD_ORIGIN, NULL_ORIGIN] + LOCAL_DEV_ORIGINS))
CORS(app, resources={r"/api/*": {"origins": final_allowed_origins}})


# --- API Routes ---
@app.route('/')
def index_route():
    return "Pusik Gifts API Backend is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            new_referral_code = f"ref_{uid}_{random.randint(1000,9999)}"
            while db.query(User).filter(User.referral_code == new_referral_code).first():
                new_referral_code = f"ref_{uid}_{random.randint(1000,9999)}"
            user = User(id=uid, username=auth.get("username"), first_name=auth.get("first_name"), last_name=auth.get("last_name"), referral_code=new_referral_code)
            db.add(user); db.commit(); db.refresh(user)
            logger.info(f"New user registered: {uid}")
        changed = False
        if user.username != auth.get("username"): user.username = auth.get("username"); changed=True
        if user.first_name != auth.get("first_name"): user.first_name = auth.get("first_name"); changed=True
        if user.last_name != auth.get("last_name"): user.last_name = auth.get("last_name"); changed=True
        if changed: db.commit(); db.refresh(user)
        inv = []
        for i in user.inventory:
            item_name = i.nft.name if i.nft else i.item_name_override
            item_image = i.nft.image_filename if i.nft else i.item_image_override or generate_image_filename_from_name(item_name)
            inv.append({
                "id":i.id, "name":item_name, "imageFilename":item_image,
                "floorPrice":i.current_value, # Use current_value of inventory item for its display
                "currentValue":i.current_value,
                "upgradeMultiplier":i.upgrade_multiplier, "variant":i.variant, "is_ton_prize":i.is_ton_prize,
                "obtained_at":i.obtained_at.isoformat() if i.obtained_at else None
            })
        refs_count = db.query(User).filter(User.referred_by_id == uid).count()
        return jsonify({
            "id":user.id, "username":user.username, "first_name":user.first_name, "last_name":user.last_name,
            "tonBalance":user.ton_balance, "starBalance":user.star_balance, "inventory":inv,
            "referralCode":user.referral_code, "referralEarningsPending":user.referral_earnings_pending,
            "total_won_ton":user.total_won_ton, "invited_friends_count":refs_count
        })
    except Exception as e: logger.error(f"Error in get_user_data for {uid}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue."}), 500
    finally: db.close()

@app.route('/api/register_referral', methods=['POST']) # No changes here
def register_referral_api():
    data = flask_request.get_json(); user_id = data.get('user_id'); username = data.get('username')
    first_name = data.get('first_name'); last_name = data.get('last_name'); referral_code_used = data.get('referral_code')
    if not all([user_id, referral_code_used]): return jsonify({"error": "Missing user_id or referral_code"}), 400
    db = next(get_db())
    try:
        referred_user = db.query(User).filter(User.id == user_id).first()
        if not referred_user:
            new_referral_code_for_user = f"ref_{user_id}_{random.randint(1000,9999)}"
            while db.query(User).filter(User.referral_code == new_referral_code_for_user).first():
                new_referral_code_for_user = f"ref_{user_id}_{random.randint(1000,9999)}"
            referred_user = User(id=user_id, username=username, first_name=first_name, last_name=last_name, referral_code=new_referral_code_for_user)
            db.add(referred_user); db.flush()
        else:
            if referred_user.username != username: referred_user.username = username
            if referred_user.first_name != first_name: referred_user.first_name = first_name
            if referred_user.last_name != last_name: referred_user.last_name = last_name
        if referred_user.referred_by_id: db.commit(); return jsonify({"status": "already_referred", "message": "User was already referred."}), 200
        referrer = db.query(User).filter(User.referral_code == referral_code_used).first()
        if not referrer: db.commit(); return jsonify({"error": "Referrer not found with this code."}), 404
        if referrer.id == referred_user.id: db.commit(); return jsonify({"error": "Cannot refer oneself."}), 400
        referred_user.referred_by_id = referrer.id; db.commit()
        logger.info(f"User {user_id} successfully referred by {referrer.id} using code {referral_code_used}")
        return jsonify({"status": "success", "message": "Referral registered successfully."}), 200
    except IntegrityError as ie: db.rollback(); logger.error(f"Integrity error registering referral for {user_id} with code {referral_code_used}: {ie}", exc_info=True); return jsonify({"error": "Database integrity error, possibly concurrent registration."}), 409
    except Exception as e: db.rollback(); logger.error(f"Error registering referral for {user_id} with code {referral_code_used}: {e}", exc_info=True); return jsonify({"error": "Server error during referral registration."}), 500
    finally: db.close()

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); cid = data.get('case_id'); multiplier = int(data.get('multiplier', 1))
    if not cid: return jsonify({"error": "case_id required"}), 400
    if multiplier not in [1,2,3]: return jsonify({"error": "Invalid multiplier. Must be 1, 2, or 3."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        
        tcase_template = next((c for c in cases_data_backend_raw_templates if c['id'] == cid), None)
        if not tcase_template: return jsonify({"error": "Case not found"}), 404
        
        live_floor_prices = get_all_live_floor_prices_from_db()
        if not live_floor_prices: return jsonify({"error": "Could not retrieve live floor prices for game calculation."}), 500

        processed_prizes_for_spin = calculate_rtp_probabilities(tcase_template, live_floor_prices)
        if not processed_prizes_for_spin: return jsonify({"error": "Failed to process case prizes for spin."}), 500
            
        base_cost = Decimal(str(tcase_template['priceTON'])); total_cost = base_cost * Decimal(multiplier)
        if Decimal(str(user.ton_balance)) < total_cost: return jsonify({"error": f"Not enough TON. Need {total_cost:.2f} TON"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - total_cost)
        won_prizes_list = []; total_value_this_spin = Decimal('0')

        for _ in range(multiplier):
            rv = random.random(); cprob = 0; chosen_prize_info = None
            for p_info in processed_prizes_for_spin:
                cprob += p_info['probability']
                if rv <= cprob: chosen_prize_info = p_info; break
            if not chosen_prize_info: chosen_prize_info = random.choice(processed_prizes_for_spin) 
            
            dbnft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
            actual_val = Decimal(str(chosen_prize_info.get('floor_price', 0))) # This is calculated by RTP
            
            is_kf_variant = chosen_prize_info['name'] in {k for k,v in INITIAL_NFT_FLOOR_PRICES_SEED.items() if "Frog" in k or "Toad" in k or "Pepe" in k or "Hop" in k or "Croak" in k} and chosen_prize_info['name'] != "Kissed Frog"
            variant_name = chosen_prize_info['name'] if is_kf_variant else None

            item = InventoryItem(user_id=uid, nft_id=dbnft.id if dbnft else None,
                                item_name_override=chosen_prize_info['name'], item_image_override=chosen_prize_info['imageFilename'],
                                current_value=float(actual_val.quantize(Decimal('0.01'))), variant=variant_name,
                                is_ton_prize=chosen_prize_info.get('is_ton_prize', False))
            db.add(item); db.flush()
            won_prizes_list.append({
                "id":item.id, "name":chosen_prize_info['name'], "imageFilename":chosen_prize_info['imageFilename'],
                "floorPrice":float(actual_val), "currentValue":item.current_value, "variant":item.variant, "is_ton_prize":item.is_ton_prize
            })
            total_value_this_spin += actual_val
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status":"success", "won_prizes":won_prizes_list, "new_balance_ton":user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Error in open_case for user {uid}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during case opening."}), 500
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
        
        target_slot_template = next((s for s in slots_data_backend_raw_templates if s['id'] == slot_id), None)
        if not target_slot_template: return jsonify({"error": "Slot not found"}), 404
        
        live_floor_prices = get_all_live_floor_prices_from_db()
        if not live_floor_prices: return jsonify({"error": "Could not retrieve live floor prices for game calculation."}), 500

        processed_prize_pool_for_spin = calculate_rtp_probabilities_for_slots(target_slot_template, live_floor_prices)
        if not processed_prize_pool_for_spin: return jsonify({"error": "Failed to process slot prizes for spin."}), 500

        cost = Decimal(str(target_slot_template['priceTON']))
        if Decimal(str(user.ton_balance)) < cost: return jsonify({"error": f"Not enough TON. Need {cost:.2f}"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - cost)
        num_reels = target_slot_template.get('reels_config', 3)
        reel_results_data = []
        for _ in range(num_reels):
            rv = random.random(); cprob = 0; landed_symbol_spec = None
            for p_info_slot in processed_prize_pool_for_spin:
                cprob += p_info_slot.get('probability', 0)
                if rv <= cprob: landed_symbol_spec = p_info_slot; break
            if not landed_symbol_spec: landed_symbol_spec = random.choice(processed_prize_pool_for_spin)
            reel_results_data.append({
                "name": landed_symbol_spec['name'], "imageFilename": landed_symbol_spec.get('imageFilename', generate_image_filename_from_name(landed_symbol_spec['name'])),
                "is_ton_prize": landed_symbol_spec.get('is_ton_prize', False),
                "currentValue": landed_symbol_spec.get('value', landed_symbol_spec.get('floor_price', 0))
            })
        won_prizes_from_slot = []; total_value_this_spin = Decimal('0')
        if num_reels == 3 and len(reel_results_data) == 3: # Assuming 3-reel match for non-TON items
            first_symbol = reel_results_data[0]
            if not first_symbol.get('is_ton_prize') and first_symbol['name'] == reel_results_data[1]['name'] and first_symbol['name'] == reel_results_data[2]['name']:
                won_item_name = first_symbol['name']
                db_nft = db.query(NFT).filter(NFT.name == won_item_name).first()
                if db_nft:
                    actual_val = Decimal(str(db_nft.floor_price)) # Use live DB price for new inventory item
                    inv_item = InventoryItem(user_id=uid, nft_id=db_nft.id, item_name_override=db_nft.name, item_image_override=db_nft.image_filename,
                                            current_value=float(actual_val.quantize(Decimal('0.01'))), variant=None, is_ton_prize=False)
                    db.add(inv_item); db.flush()
                    won_prizes_from_slot.append({
                        "id": inv_item.id, "name": inv_item.item_name_override, "imageFilename": inv_item.item_image_override,
                        "floorPrice": float(db_nft.floor_price), "currentValue": inv_item.current_value, "is_ton_prize": False, "variant": inv_item.variant
                    })
                    total_value_this_spin += actual_val
                else: logger.error(f"Slot win: NFT '{won_item_name}' not found in DB! Cannot add to inventory.")
        for landed_item_data in reel_results_data: # Add TON prizes from any reel
            if landed_item_data.get('is_ton_prize'):
                ton_val = Decimal(str(landed_item_data['currentValue']))
                user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_val)
                total_value_this_spin += ton_val
                won_prizes_from_slot.append({
                    "id": f"ton_prize_{int(time.time()*1e6)}_{random.randint(0,99999)}", "name": landed_item_data['name'],
                    "imageFilename": landed_item_data.get('imageFilename', TON_PRIZE_IMAGE_DEFAULT), "currentValue": float(ton_val), "is_ton_prize": True
                })
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status":"success", "reel_results":reel_results_data, "won_prizes":won_prizes_from_slot, "new_balance_ton":user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Error in spin_slot for user {uid}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during slot spin."}), 500
    finally: db.close()


@app.route('/api/upgrade_item', methods=['POST']) # No changes here
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id'); mult_str = data.get('multiplier_str')
    if not all([iid, mult_str]): return jsonify({"error": "Missing inventory_item_id or multiplier_str parameter."}), 400
    try: mult = Decimal(mult_str); iid_int = int(iid)
    except (ValueError, TypeError): return jsonify({"error": "Invalid data format for multiplier or item ID."}), 400
    chances = {Decimal("1.5"):50, Decimal("2.0"):35, Decimal("3.0"):25, Decimal("5.0"):15, Decimal("10.0"):8, Decimal("20.0"):3}
    if mult not in chances: return jsonify({"error": "Invalid multiplier value provided."}), 400
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_int, InventoryItem.user_id == uid).with_for_update().first()
        if not item or item.is_ton_prize: return jsonify({"error": "Item not found in your inventory or cannot be upgraded."}), 404
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found."}), 404
        if random.uniform(0,100) < chances[mult]:
            orig_val = Decimal(str(item.current_value)); new_val = (orig_val * mult).quantize(Decimal('0.01'), ROUND_HALF_UP)
            increase_in_value = new_val - orig_val
            item.current_value = float(new_val); item.upgrade_multiplier = float(Decimal(str(item.upgrade_multiplier)) * mult)
            user.total_won_ton = float(Decimal(str(user.total_won_ton)) + increase_in_value)
            db.commit()
            return jsonify({"status":"success", "message":f"Upgrade successful! Your {item.item_name_override or item.nft.name} is now worth {new_val:.2f} TON.",
                            "item":{"id":item.id, "currentValue":item.current_value, "name":item.nft.name if item.nft else item.item_name_override,
                                    "imageFilename":item.nft.image_filename if item.nft else item.item_image_override,
                                    "upgradeMultiplier":item.upgrade_multiplier, "variant":item.variant}})
        else:
            name_lost = item.nft.name if item.nft else item.item_name_override; value_lost = Decimal(str(item.current_value))
            user.total_won_ton = float(max(Decimal('0'), Decimal(str(user.total_won_ton)) - value_lost))
            db.delete(item); db.commit()
            return jsonify({"status":"failed","message":f"Upgrade failed! You lost your {name_lost}.", "item_lost":True})
    except Exception as e: db.rollback(); logger.error(f"Error in upgrade_item for user {uid}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during upgrade."}), 500
    finally: db.close()

@app.route('/api/convert_to_ton', methods=['POST']) # No changes here
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid_convert = data.get('inventory_item_id')
    if not iid_convert: return jsonify({"error": "inventory_item_id required."}), 400
    try: iid_convert_int = int(iid_convert)
    except ValueError: return jsonify({"error": "Invalid inventory_item_id format."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_convert_int, InventoryItem.user_id == uid).first()
        if not user: return jsonify({"error": "User not found."}), 404
        if not item: return jsonify({"error": "Item not found in your inventory."}), 404
        if item.is_ton_prize: return jsonify({"error": "Cannot convert a TON prize item."}), 400
        val_to_add = Decimal(str(item.current_value))
        user.ton_balance = float(Decimal(str(user.ton_balance)) + val_to_add)
        item_name_converted = item.nft.name if item.nft else item.item_name_override
        user.total_won_ton = float(max(Decimal('0'), Decimal(str(user.total_won_ton)) - val_to_add))
        db.delete(item); db.commit()
        return jsonify({"status":"success", "message":f"Item '{item_name_converted}' converted to {val_to_add:.2f} TON.", "new_balance_ton":user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Error in convert_to_ton for user {uid}, item {iid_convert_int}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during conversion."}), 500
    finally: db.close()

@app.route('/api/sell_all_items', methods=['POST']) # No changes here
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        items_to_sell = [item_obj for item_obj in user.inventory if not item_obj.is_ton_prize]
        if not items_to_sell: return jsonify({"status":"no_items","message":"No sellable items in your collection to convert."})
        total_value_from_sell = sum(Decimal(str(i_sell.current_value)) for i_sell in items_to_sell)
        user.ton_balance = float(Decimal(str(user.ton_balance)) + total_value_from_sell)
        num_items_sold = len(items_to_sell)
        user.total_won_ton = float(max(Decimal('0'), Decimal(str(user.total_won_ton)) - total_value_from_sell))
        for i_del in items_to_sell: db.delete(i_del)
        db.commit()
        return jsonify({"status":"success", "message":f"All {num_items_sold} sellable items converted for a total of {total_value_from_sell:.2f} TON.", "new_balance_ton":user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Error in sell_all_items for user {uid}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during bulk conversion."}), 500
    finally: db.close()

@app.route('/api/initiate_deposit', methods=['POST']) # No changes here
def initiate_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); amt_str = data.get('amount')
    if amt_str is None: return jsonify({"error": "Amount required."}), 400
    try: orig_amt = float(amt_str)
    except ValueError: return jsonify({"error": "Invalid amount format."}), 400
    if not (0.1 <= orig_amt <= 10000): return jsonify({"error": "Amount must be between 0.1 and 10000 TON."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user: return jsonify({"error": "User not found."}), 404
        existing_pending_deposit = db.query(PendingDeposit).filter(PendingDeposit.user_id == uid, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first()
        if existing_pending_deposit:
            return jsonify({"error": "You already have an active deposit. Please wait for it to expire or complete.",
                            "pending_deposit_id": existing_pending_deposit.id, "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW,
                            "amount_to_send": f"{existing_pending_deposit.final_amount_nano_ton / 1e9:.9f}".rstrip('0').rstrip('.'),
                            "final_amount_nano_ton": existing_pending_deposit.final_amount_nano_ton,
                            "comment": existing_pending_deposit.expected_comment, "expires_at": existing_pending_deposit.expires_at.isoformat()}), 409
        nano_part = random.randint(10000, 999999); final_nano_amt = int(orig_amt * 1e9) + nano_part
        pdep = PendingDeposit(user_id=uid, original_amount_ton=orig_amt, unique_identifier_nano_ton=nano_part, final_amount_nano_ton=final_nano_amt,
                              expected_comment=DEPOSIT_COMMENT, expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
        db.add(pdep); db.commit(); db.refresh(pdep)
        amount_to_send_display = f"{final_nano_amt / 1e9:.9f}".rstrip('0').rstrip('.')
        return jsonify({"status":"success", "pending_deposit_id":pdep.id, "recipient_address":DEPOSIT_RECIPIENT_ADDRESS_RAW,
                        "amount_to_send":amount_to_send_display, "final_amount_nano_ton":final_nano_amt,
                        "comment":DEPOSIT_COMMENT, "expires_at":pdep.expires_at.isoformat()})
    except Exception as e: db.rollback(); logger.error(f"Error in initiate_deposit for user {uid}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during deposit initiation."}), 500
    finally: db.close()

async def check_blockchain_for_deposit(pdep: PendingDeposit, db_sess: SessionLocal): # No changes here
    prov = None
    try:
        prov = LiteBalancer.from_mainnet_config(trust_level=2); await prov.start_up()
        txs = await prov.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=50)
        deposit_found = False
        for tx in txs:
            if not tx.in_msg or not tx.in_msg.is_internal: continue
            if tx.in_msg.info.value_coins != pdep.final_amount_nano_ton: continue
            tx_time = dt.fromtimestamp(tx.now, tz=timezone.utc)
            if not (pdep.created_at - timedelta(minutes=5) <= tx_time <= pdep.expires_at + timedelta(minutes=5)): continue
            cmt_slice = tx.in_msg.body.begin_parse()
            if cmt_slice.remaining_bits >= 32 and cmt_slice.load_uint(32) == 0:
                try:
                    comment_text = cmt_slice.load_snake_string()
                    if comment_text == pdep.expected_comment: deposit_found = True; break
                except Exception as e_comment: logger.debug(f"Comment parsing error for tx {tx.hash.hex()}: {e_comment}")
        if deposit_found:
            usr = db_sess.query(User).filter(User.id == pdep.user_id).with_for_update().first()
            if not usr: pdep.status = 'failed_user_not_found'; db_sess.commit(); logger.error(f"Deposit {pdep.id} confirmed but user {pdep.user_id} not found."); return {"status":"error","message":"User for deposit not found."}
            usr.ton_balance = float(Decimal(str(usr.ton_balance)) + Decimal(str(pdep.original_amount_ton)))
            if usr.referred_by_id:
                referrer = db_sess.query(User).filter(User.id == usr.referred_by_id).with_for_update().first()
                if referrer:
                    referral_bonus = (Decimal(str(pdep.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                    referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + referral_bonus)
                    logger.info(f"Referral bonus of {referral_bonus:.2f} TON added to user {referrer.id} for deposit {pdep.id}.")
            pdep.status = 'completed'; db_sess.commit()
            logger.info(f"Deposit {pdep.id} (TON: {pdep.original_amount_ton}) confirmed and credited to user {usr.id}.")
            return {"status":"success","message":"Deposit confirmed and credited!","new_balance_ton":usr.ton_balance}
        else:
            if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending':
                pdep.status = 'expired'; db_sess.commit()
                logger.info(f"Deposit {pdep.id} expired for user {pdep.user_id}.")
                return {"status":"expired","message":"This deposit request has expired."}
            return {"status":"pending","message":"Transaction not confirmed yet. Please wait or check again."}
    except Exception as e_bc_check: logger.error(f"Blockchain check error for deposit {pdep.id}: {e_bc_check}", exc_info=True); return {"status":"error","message":"An error occurred during blockchain verification."}
    finally:
        if prov: await prov.close_all()

@app.route('/api/verify_deposit', methods=['POST']) # No changes here
def verify_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); pid = data.get('pending_deposit_id')
    if not pid: return jsonify({"error": "Pending deposit ID required."}), 400
    db = next(get_db())
    try:
        pdep = db.query(PendingDeposit).filter(PendingDeposit.id == pid, PendingDeposit.user_id == uid).with_for_update().first()
        if not pdep: return jsonify({"error": "Pending deposit not found or does not belong to your account."}), 404
        if pdep.status == 'completed': usr = db.query(User).filter(User.id == uid).first(); return jsonify({"status":"success","message":"Deposit was already confirmed and credited.","new_balance_ton":usr.ton_balance if usr else 0})
        if pdep.status == 'pending' and pdep.expires_at <= dt.now(timezone.utc): pdep.status = 'expired'; db.commit(); logger.info(f"Deposit {pdep.id} marked as expired on verification."); return jsonify({"status":"expired","message":"This deposit request has expired."}), 400
        result = {}; loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        try: result = loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
        except Exception as e_async_exec: logger.error(f"Async exec error during verify_deposit for {pid}: {e_async_exec}", exc_info=True); return jsonify({"status":"error","message":"Server error during verification. Try again."}), 500
        finally: loop.close()
        return jsonify(result)
    except Exception as e_outer: db.rollback(); logger.error(f"Outer error in verify_deposit for {pid}: {e_outer}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during deposit verification."}), 500
    finally: db.close()

@app.route('/api/get_leaderboard', methods=['GET']) # No changes here
def get_leaderboard_api():
    db = next(get_db())
    try:
        leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        leaderboard_data = []
        for r_idx, u_leader in enumerate(leaders):
            display_name = u_leader.first_name or u_leader.username or f"User_{str(u_leader.id)[:6]}"
            avatar_char = (u_leader.first_name or u_leader.username or "U")[0].upper()
            leaderboard_data.append({"rank": r_idx + 1, "name": display_name, "avatarChar": avatar_char, "income": u_leader.total_won_ton, "user_id": u_leader.id})
        return jsonify(leaderboard_data)
    except Exception as e: logger.error(f"Error in get_leaderboard: {e}", exc_info=True); return jsonify({"error":"Could not load leaderboard due to a server error."}), 500
    finally: db.close()

@app.route('/api/withdraw_referral_earnings', methods=['POST']) # No changes here
def withdraw_referral_earnings_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found."}), 404
        if user.referral_earnings_pending > 0:
            withdrawn_amount = Decimal(str(user.referral_earnings_pending))
            user.ton_balance = float(Decimal(str(user.ton_balance)) + withdrawn_amount)
            user.referral_earnings_pending = 0.0; db.commit()
            return jsonify({"status":"success", "message":f"{withdrawn_amount:.2f} TON referral earnings withdrawn.", "new_balance_ton":user.ton_balance, "new_referral_earnings_pending":0.0})
        else: return jsonify({"status":"no_earnings","message":"No referral earnings to withdraw."})
    except Exception as e: db.rollback(); logger.error(f"Error withdrawing referral earnings for user {uid}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue during withdrawal."}), 500
    finally: db.close()

@app.route('/api/redeem_promocode', methods=['POST']) # No changes here
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); code_txt = data.get('promocode_text', "").strip()
    if not code_txt: return jsonify({"status":"error","message":"Promocode text cannot be empty."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"status":"error","message":"User not found."}), 404
        promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).with_for_update().first()
        if not promo: return jsonify({"status":"error","message":"Invalid promocode."}), 404
        if promo.activations_left != -1 and promo.activations_left <= 0: return jsonify({"status":"error","message":"This promocode has no activations left."}), 400
        existing_redemption = db.query(UserPromoCodeRedemption).filter(UserPromoCodeRedemption.user_id == user.id, UserPromoCodeRedemption.promo_code_id == promo.id).first()
        if existing_redemption: return jsonify({"status":"error","message":"You have already redeemed this promocode."}), 400
        if promo.activations_left != -1: promo.activations_left -= 1
        user.ton_balance = float(Decimal(str(user.ton_balance)) + Decimal(str(promo.ton_amount)))
        new_redemption = UserPromoCodeRedemption(user_id=user.id, promo_code_id=promo.id)
        db.add(new_redemption); db.commit()
        return jsonify({"status":"success", "message":f"Promocode '{code_txt}' redeemed! You received {promo.ton_amount:.2f} TON.", "new_balance_ton":user.ton_balance})
    except IntegrityError as ie: db.rollback(); logger.error(f"IntegrityError redeeming promocode '{code_txt}' for user {uid}: {ie}", exc_info=True); return jsonify({"status":"error","message":"Promocode redemption failed due to conflict. Try again."}), 409
    except Exception as e: db.rollback(); logger.error(f"Error redeeming promocode '{code_txt}' for user {uid}: {e}", exc_info=True); return jsonify({"status":"error","message":"Server error during promocode redemption."}), 500
    finally: db.close()

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST']) # No changes here
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"status":"error","message":"Authentication failed"}), 401
    player_user_id = auth_user_data["id"]
    if not TONNEL_SENDER_INIT_DATA: logger.error("Tonnel withdrawal: TONNEL_SENDER_INIT_DATA not set."); return jsonify({"status":"error","message":"Withdrawal service unavailable."}), 500
    if not TONNEL_GIFT_SECRET: logger.error("Tonnel withdrawal: TONNEL_GIFT_SECRET not set."); return jsonify({"status":"error","message":"Withdrawal service misconfigured."}), 500
    db = next(get_db())
    try:
        item_to_withdraw = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == player_user_id).with_for_update().first()
        if not item_to_withdraw: return jsonify({"status":"error","message":"Item not found in your inventory."}), 404
        if item_to_withdraw.is_ton_prize: return jsonify({"status":"error","message":"TON prizes cannot be withdrawn as gifts."}), 400
        item_name_for_tonnel = item_to_withdraw.item_name_override or (item_to_withdraw.nft.name if item_to_withdraw.nft else None)
        if not item_name_for_tonnel: logger.error(f"Item {inventory_item_id} has no name for Tonnel withdrawal for user {player_user_id}."); return jsonify({"status":"error","message":"Item data incomplete."}), 500
        tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        tonnel_result = {}; loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        try: tonnel_result = loop.run_until_complete(tonnel_client.send_gift_to_user(gift_item_name=item_name_for_tonnel, receiver_telegram_id=player_user_id))
        finally: loop.close(); asyncio.set_event_loop(asyncio.get_event_loop()) # Restore original event loop if any
        if tonnel_result and tonnel_result.get("status") == "success":
            value_deducted_from_winnings = Decimal(str(item_to_withdraw.current_value))
            player = db.query(User).filter(User.id == player_user_id).with_for_update().first()
            if player: player.total_won_ton = float(max(Decimal('0'), Decimal(str(player.total_won_ton)) - value_deducted_from_winnings))
            db.delete(item_to_withdraw); db.commit()
            logger.info(f"Item {item_name_for_tonnel} (ID: {inventory_item_id}) withdrawn via Tonnel for user {player_user_id}.")
            return jsonify({"status":"success", "message":f"Your gift '{item_name_for_tonnel}' sent via Tonnel!", "details":tonnel_result.get("details")})
        else:
            db.rollback(); logger.error(f"Tonnel withdrawal failed for item {inventory_item_id}, user {player_user_id}. Tonnel API Response: {tonnel_result}")
            return jsonify({"status":"error","message":f"Withdrawal failed: {tonnel_result.get('message', 'Tonnel API error')}"}), 500
    except Exception as e: db.rollback(); logger.error(f"Unexpected exception during Tonnel withdrawal for item {inventory_item_id}, user {player_user_id}: {e}", exc_info=True); return jsonify({"status":"error","message":"Unexpected server error during withdrawal."}), 500
    finally: db.close()

# --- NEW ENDPOINTS ---
@app.route('/api/update_floor_price', methods=['POST'])
def update_floor_price_api():
    auth_header = flask_request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization header missing or malformed."}), 401
    received_key = auth_header.split('Bearer ')[1]
    if received_key != API_KEY_SECRET:
        return jsonify({"error": "Invalid API Key."}), 403
    data = flask_request.get_json()
    gift_name = data.get('gift_name'); new_floor_price = data.get('new_floor_price')
    if not all([gift_name, new_floor_price is not None]):
        return jsonify({"error": "Missing gift_name or new_floor_price."}), 400
    try:
        new_floor_price_float = float(new_floor_price)
        if new_floor_price_float < 0: return jsonify({"error": "Floor price cannot be negative."}), 400
    except ValueError: return jsonify({"error": "new_floor_price must be a valid number."}), 400
    db = next(get_db())
    try:
        nft = db.query(NFT).filter(NFT.name == gift_name).first()
        if not nft:
            image_filename = generate_image_filename_from_name(gift_name)
            new_nft = NFT(name=gift_name, image_filename=image_filename, floor_price=new_floor_price_float)
            db.add(new_nft); db.commit()
            logger.info(f"Created new NFT '{gift_name}' with floor price {new_floor_price_float}.")
            return jsonify({"status": "success", "message": f"NFT '{gift_name}' created and floor price set to {new_floor_price_float}."})
        else:
            if nft.floor_price != new_floor_price_float:
                old_price = nft.floor_price; nft.floor_price = new_floor_price_float; db.commit()
                logger.info(f"Updated floor price for NFT '{gift_name}': {old_price} -> {new_floor_price_float}.")
                return jsonify({"status": "success", "message": f"Floor price for '{gift_name}' updated to {new_floor_price_float}."})
            else:
                return jsonify({"status": "noop", "message": f"Floor price for '{gift_name}' is already {new_floor_price_float}."})
    except Exception as e: db.rollback(); logger.error(f"Error updating floor price for {gift_name}: {e}", exc_info=True); return jsonify({"error": "Database error or unexpected issue."}), 500
    finally: db.close()

@app.route('/api/get_all_floor_prices', methods=['GET'])
def get_all_floor_prices_api():
    floor_prices = get_all_live_floor_prices_from_db()
    if not floor_prices and not INITIAL_NFT_FLOOR_PRICES_SEED: # Only error if seed is also empty (should not happen)
        return jsonify({"error": "Could not retrieve floor prices."}), 500
    elif not floor_prices and INITIAL_NFT_FLOOR_PRICES_SEED:
        logger.warning("get_all_floor_prices_api: No floor prices in DB, returning seed prices as fallback.")
        return jsonify(INITIAL_NFT_FLOOR_PRICES_SEED) # Fallback to seed if DB is empty after init
    return jsonify(floor_prices)

initial_setup_and_logging() # Call this after all definitions

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=True)
