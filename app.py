import os
import logging
from flask import Flask, jsonify, request as flask_request, abort as flask_abort
from flask_cors import CORS
import telebot
from telebot import types
from dotenv import load_dotenv
import time
import random
import re
import hmac
import hashlib
from urllib.parse import unquote, parse_qs
from datetime import datetime as dt, timezone, timedelta
import json
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, DateTime, Boolean, UniqueConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, declarative_base, joinedload
from sqlalchemy.sql import func
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from curl_cffi.requests import AsyncSession, RequestsError # Keep if used by TonnelGiftSender
import base64
from Crypto.Cipher import AES 
from Crypto.Random import get_random_bytes 
from Crypto.Util.Padding import pad 
from pytoniq import LiteBalancer 
import asyncio

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_NAME = os.environ.get("MINI_APP_NAME", "case") 
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24 
TONNEL_SENDER_INIT_DATA = os.environ.get("TONNEL_SENDER_INIT_DATA")
TONNEL_GIFT_SECRET = os.environ.get("TONNEL_GIFT_SECRET", "yowtfisthispieceofshitiiit")
WEBHOOK_URL_BASE = os.environ.get("WEBHOOK_URL_BASE")
WEBAPP_URL = "https://vasiliy-katsyka.github.io/maintencaincec"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("app.log", encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN: logger.error("BOT_TOKEN not set!"); exit("BOT_TOKEN is not set. Exiting.")
if not DATABASE_URL: logger.error("DATABASE_URL not set!"); exit("DATABASE_URL is not set. Exiting.")
if not TONNEL_SENDER_INIT_DATA: logger.warning("TONNEL_SENDER_INIT_DATA not set! Tonnel gift withdrawal will likely fail.")

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
    star_balance = Column(Integer, default=0, nullable=False) # Not used currently but kept
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
    name = Column(String, unique=True, index=True, nullable=False) # Will store "Model Name (X%)" for frogs
    image_filename = Column(String, nullable=True) 
    floor_price = Column(Float, default=0.0, nullable=False)
    base_gift_name = Column(String, nullable=True) # e.g., "Kissed Frog"
    model_name_only = Column(String, nullable=True) # e.g., "Happy Pepe"
    __table_args__ = (UniqueConstraint('name', name='uq_nft_name'),)

class InventoryItem(Base):
    __tablename__ = "inventory_items"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    nft_id = Column(Integer, ForeignKey("nfts.id"), nullable=True) # Points to NFT entry with "Model (X%)" name
    item_name_override = Column(String, nullable=True) # Less used now, nft.name is primary
    item_image_override = Column(String, nullable=True) # Less used
    current_value = Column(Float, nullable=False)
    upgrade_multiplier = Column(Float, default=1.0, nullable=False)
    obtained_at = Column(DateTime(timezone=True), server_default=func.now())
    variant = Column(String, nullable=True) # Will store just "Model Name" for frogs, for Tonnel model lookup
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

# --- TonnelGiftSender (AES part remains the same) ---
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

class TonnelGiftSender:
    def __init__(self, sender_auth_data: str, gift_secret_passphrase: str):
        self.passphrase_secret = gift_secret_passphrase; self.authdata = sender_auth_data; self._session_instance: AsyncSession | None = None
    async def _get_session(self) -> AsyncSession:
        if self._session_instance is None: self._session_instance = AsyncSession(impersonate="chrome110")
        return self._session_instance
    async def _close_session_if_open(self):
        if self._session_instance:
            try: await self._session_instance.close()
            except Exception as e_close: logger.error(f"Error closing AsyncSession: {e_close}")
            finally: self._session_instance = None
    async def _make_request(self, method: str, url: str, headers: dict | None = None, json_payload: dict | None = None, timeout: int = 30, is_initial_get: bool = False):
        session = await self._get_session(); response_obj = None
        try:
            request_kwargs = {"headers": headers, "timeout": timeout}
            if json_payload is not None and method.upper() == "POST": request_kwargs["json"] = json_payload
            if method.upper() == "GET": response_obj = await session.get(url, **request_kwargs)
            elif method.upper() == "POST": response_obj = await session.post(url, **request_kwargs)
            elif method.upper() == "OPTIONS": response_obj = await session.options(url, **request_kwargs)
            else: raise ValueError(f"Unsupported HTTP method: {method}")
            if method.upper() == "OPTIONS":
                if 200 <= response_obj.status_code < 300: return {"status": "options_ok"}
                else: err_text_options = await response_obj.text(); logger.error(f"Tonnel API OPTIONS {url} failed: {response_obj.status_code}. Resp: {err_text_options[:500]}"); response_obj.raise_for_status(); return {"status": "error", "message": f"OPTIONS failed: {response_obj.status_code}"}
            response_obj.raise_for_status()
            if response_obj.status_code == 204: return None
            content_type = response_obj.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                try: return response_obj.json()
                except json.JSONDecodeError as je_err_inner: err_text_json_decode = await response_obj.text(); logger.error(f"Tonnel API JSONDecodeError for {method} {url}: {je_err_inner}. Body: {err_text_json_decode[:500]}"); return {"status": "error", "message": "Invalid JSON in response", "raw_text": err_text_json_decode[:500]}
            else:
                if is_initial_get: return {"status": "get_ok_non_json"}
                else: responseText = await response_obj.text(); logger.warning(f"Tonnel API {method} {url} - Non-JSON (Type: {content_type}). Text: {responseText[:200]}"); return {"status": "error", "message": "Response not JSON", "content_type": content_type, "text_preview": responseText[:200]}
        except RequestsError as re_err: logger.error(f"Tonnel API RequestsError ({method} {url}): {re_err}"); raise
        except json.JSONDecodeError as je_err: logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err}"); raise ValueError(f"Failed to decode JSON from {url}") from je_err
        except Exception as e_gen: logger.error(f"Tonnel API general request error ({method} {url}): {type(e_gen).__name__} - {e_gen}"); raise

    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int, gift_model_name: str | None = None):
        if not self.authdata: return {"status": "error", "message": "Tonnel sender not configured."}
        try:
            await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)
            base_filter_dict = {"price":{"$exists":True},"refunded":{"$ne":True},"buyer":{"$exists":False},"export_at":{"$exists":True},"gift_name":gift_item_name,"asset":"TON"}
            if gift_item_name == "Kissed Frog" and gift_model_name: # gift_model_name is now just "Model Name"
                base_filter_dict["model"] = gift_model_name
                logger.info(f"Tonnel: Filtering for gift_name='{gift_item_name}', model='{gift_model_name}'")
            else: logger.info(f"Tonnel: Filtering for gift_name='{gift_item_name}' (no specific model or not Kissed Frog)")
            filter_str = json.dumps(base_filter_dict)
            page_gifts_payload={"filter":filter_str,"limit":10,"page":1,"sort":'{"price":1,"gift_id":-1}'}
            pg_headers_options={"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type","Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"} # Standardized
            pg_headers_post={"Content-Type":"application/json","Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}
            await self._make_request(method="OPTIONS",url="https://gifts2.tonnel.network/api/pageGifts",headers=pg_headers_options)
            gifts_found_response=await self._make_request(method="POST",url="https://gifts2.tonnel.network/api/pageGifts",headers=pg_headers_post,json_payload=page_gifts_payload)
            if not isinstance(gifts_found_response,list): err_msg = f"Could not fetch gift list: {gifts_found_response.get('message','API error') if isinstance(gifts_found_response,dict) else 'Format error'}"; logger.error(f"Tonnel: {err_msg}. Payload: {page_gifts_payload}"); return {"status":"error","message":err_msg}
            if not gifts_found_response:
                no_gift_msg = f"No '{gift_item_name}'";
                if gift_model_name: no_gift_msg += f" (model: '{gift_model_name}')"
                no_gift_msg += " gifts available on Tonnel."
                logger.warning(f"Tonnel: {no_gift_msg}. Payload: {page_gifts_payload}"); return {"status":"error","message":no_gift_msg}
            low_gift=gifts_found_response[0]
            user_info_payload={"authData":self.authdata,"user":receiver_telegram_id}
            ui_common_headers={"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}
            ui_options_headers={**ui_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            ui_post_headers={**ui_common_headers,"Content-Type":"application/json"}
            await self._make_request(method="OPTIONS",url="https://gifts2.tonnel.network/api/userInfo",headers=ui_options_headers)
            user_check_resp=await self._make_request(method="POST",url="https://gifts2.tonnel.network/api/userInfo",headers=ui_post_headers,json_payload=user_info_payload)
            if not isinstance(user_check_resp,dict)or user_check_resp.get("status")!="success": return {"status":"error","message":f"Tonnel user check failed: {user_check_resp.get('message','User error') if isinstance(user_check_resp,dict) else 'Unknown error'}"}
            encrypted_ts=encrypt_aes_cryptojs_compat(f"{int(time.time())}",self.passphrase_secret)
            buy_gift_url=f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}"
            buy_payload={"anonymously":True,"asset":"TON","authData":self.authdata,"price":low_gift['price'],"receiver":receiver_telegram_id,"showPrice":False,"timestamp":encrypted_ts}
            buy_common_headers={"Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/","Host":"gifts.coffin.meme"}
            buy_options_headers={**buy_common_headers,"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type"}
            buy_post_headers={**buy_common_headers,"Content-Type":"application/json"}
            await self._make_request(method="OPTIONS",url=buy_gift_url,headers=buy_options_headers)
            purchase_resp=await self._make_request(method="POST",url=buy_gift_url,headers=buy_post_headers,json_payload=buy_payload,timeout=90)
            if isinstance(purchase_resp,dict)and purchase_resp.get("status")=="success": return {"status":"success","message":f"Gift '{gift_item_name}' sent!","details":purchase_resp}
            else: return {"status":"error","message":f"Tonnel transfer failed: {purchase_resp.get('message','Purchase error') if isinstance(purchase_resp,dict) else 'Unknown error'}"}
        except Exception as e:
            model_info_for_log = f"(model: {gift_model_name})" if gift_model_name else ""; logger.error(f"Tonnel error sending gift '{gift_item_name}' {model_info_for_log} to {receiver_telegram_id}: {type(e).__name__} - {e}", exc_info=True); return {"status":"error","message":f"Unexpected error during Tonnel withdrawal: {str(e)}"}
        finally: await self._close_session_if_open()

TON_PRIZE_IMAGE_DEFAULT = "https://case-bot.com/images/actions/ton.svg" 
GIFT_NAME_TO_ID_MAP_PY = { "Santa Hat": "5983471780763796287","Signet Ring": "5936085638515261992","Precious Peach": "5933671725160989227","Plush Pepe": "5936013938331222567","Spiced Wine": "5913442287462908725","Jelly Bunny": "5915502858152706668","Durov's Cap": "5915521180483191380","Perfume Bottle": "5913517067138499193","Eternal Rose": "5882125812596999035","Berry Box": "5882252952218894938","Vintage Cigar": "5857140566201991735","Magic Potion": "5846226946928673709", "Kissed Frog": "5845776576658015084","Hex Pot": "5825801628657124140","Evil Eye": "5825480571261813595","Sharp Tongue": "5841689550203650524","Trapped Heart": "5841391256135008713","Skull Flower": "5839038009193792264","Scared Cat": "5837059369300132790","Spy Agaric": "5821261908354794038","Homemade Cake": "5783075783622787539","Genie Lamp": "5933531623327795414","Lunar Snake": "6028426950047957932","Party Sparkler": "6003643167683903930","Jester Hat": "5933590374185435592","Witch Hat": "5821384757304362229","Hanging Star": "5915733223018594841","Love Candle": "5915550639663874519","Cookie Heart": "6001538689543439169","Desk Calendar": "5782988952268964995","Jingle Bells": "6001473264306619020","Snow Mittens": "5980789805615678057","Voodoo Doll": "5836780359634649414","Mad Pumpkin": "5841632504448025405","Hypno Lollipop": "5825895989088617224", "B-Day Candle": "5782984811920491178","Bunny Muffin": "5935936766358847989","Astral Shard": "5933629604416717361","Flying Broom": "5837063436634161765","Crystal Ball": "5841336413697606412","Eternal Candle": "5821205665758053411","Swiss Watch": "5936043693864651359","Ginger Cookie": "5983484377902875708","Mini Oscar": "5879737836550226478","Lol Pop": "5170594532177215681","Ion Gem": "5843762284240831056","Star Notepad": "5936017773737018241","Loot Bag": "5868659926187901653","Love Potion": "5868348541058942091","Toy Bear": "5868220813026526561","Diamond Ring": "5868503709637411929","Sakura Flower": "5167939598143193218","Sleigh Bell": "5981026247860290310","Top Hat": "5897593557492957738","Record Player": "5856973938650776169","Winter Wreath": "5983259145522906006","Snow Globe": "5981132629905245483","Electric Skull": "5846192273657692751","Tama Gadget": "6023752243218481939","Candy Cane": "6003373314888696650","Neko Helmet": "5933793770951673155","Jack-in-the-Box": "6005659564635063386", "Easter Egg": "5773668482394620318","Bonded Ring": "5870661333703197240","Pet Snake": "6023917088358269866","Snake Box": "6023679164349940429","Xmas Stocking": "6003767644426076664","Big Year": "6028283532500009446","Holiday Drink": "6003735372041814769","Gem Signet": "5859442703032386168","Light Sword": "5897581235231785485"
}
GIFT_NAME_TO_ID_MAP_PY["Durov's Cap"] = "5915521180483191380" # Explicit for this specific case

# Updated Kissed Frog Model Data (with percentages in names for display/DB, and probabilities)
KISSED_FROG_MODELS_WITH_PERCENT_PROB = {
    "Brewtoad (0.5%)": 0.005, "Zodiak Croak (0.5%)": 0.005, "Rocky Hopper (0.5%)": 0.005,
    "Puddles (0.5%)": 0.005, "Lucifrog (0.5%)": 0.005, "Honeyhop (0.5%)": 0.005,
    "Count Croakula (0.5%)": 0.005, "Lilie Pond (0.5%)": 0.005, "Frogmaid (0.5%)": 0.005, # Note: "Lilie" vs "Lily"
    "Happy Pepe (0.5%)": 0.005, "Melty Butter (0.5%)": 0.005, "Sweet Dream (0.5%)": 0.005,
    "Tree Frog (0.5%)": 0.005,
    "Lava Leap (1%)": 0.01, "Tesla Frog (1%)": 0.01, "Trixie (1%)": 0.01,
    "Pond Fairy (1%)": 0.01, "Icefrog (1%)": 0.01,
    "Hopberry (1.5%)": 0.015, "Boingo (1.5%)": 0.015, "Prince Ribbit (1.5%)": 0.015,
    "Toadstool (1.5%)": 0.015, "Cupid (1.5%)": 0.015, "Ms. Toad (1.5%)": 0.015,
    "Desert Frog (1.5%)": 0.015,
    "Silver (2%)": 0.02, "Bronze (2%)": 0.02,
    "Poison (2.5%)": 0.025, "Ramune (2.5%)": 0.025, "Lemon Drop (2.5%)": 0.025,
    "Minty Bloom (2.5%)": 0.025, "Void Hopper (2.5%)": 0.025, "Sarutoad (2.5%)": 0.025,
    "Duskhopper (2.5%)": 0.025, "Starry Night (2.5%)": 0.025, "Ectofrog (2.5%)": 0.025,
    "Ectobloom (2.5%)": 0.025,
    "Melon (3%)": 0.03, "Banana Pox (3%)": 0.03, "Frogtart (3%)": 0.03,
    "Sea Breeze (4%)": 0.04, "Sky Leaper (4%)": 0.04, "Toadberry (4%)": 0.04,
    "Peach (4%)": 0.04, "Lily Pond (4%)": 0.04, "Frogwave (4%)": 0.04, # Note: "Lily" vs "Lilie"
    "Cranberry (4%)": 0.04, "Lemon Juice (4%)": 0.04, "Tide Pod (4%)": 0.04,
    "Brownie (4%)": 0.04,
}

# Helper to get model name without percentage
def strip_percentage_from_name(name_with_percentage: str) -> str:
    match = re.match(r"^(.*?)\s*\(\d+(\.\d+)?%\)$", name_with_percentage)
    return match.group(1).strip() if match else name_with_percentage

# Base floor prices for Kissed Frog models (model name WITHOUT percentage is key)
KISSED_FROG_VARIANT_BASE_FLOORS = {"Happy Pepe":500.0,"Tree Frog":150.0,"Brewtoad":150.0,"Puddles":150.0,"Honeyhop":150.0,"Melty Butter":150.0,"Lucifrog":150.0,"Zodiak Croak":150.0,"Count Croakula":150.0,"Lilie Pond":150.0,"Sweet Dream":150.0,"Frogmaid":150.0,"Rocky Hopper":150.0,"Icefrog":45.0,"Lava Leap":45.0,"Toadstool":45.0,"Desert Frog":45.0,"Cupid":45.0,"Hopberry":45.0,"Ms. Toad":45.0,"Trixie":45.0,"Prince Ribbit":45.0,"Pond Fairy":45.0,"Boingo":45.0,"Tesla Frog":45.0,"Starry Night":30.0,"Silver":30.0,"Ectofrog":30.0,"Poison":30.0,"Minty Bloom":30.0,"Sarutoad":30.0,"Void Hopper":30.0,"Ramune":30.0,"Lemon Drop":30.0,"Ectobloom":30.0,"Duskhopper":30.0,"Bronze":30.0,"Lily Pond":19.0,"Toadberry":19.0,"Frogwave":19.0,"Melon":19.0,"Sky Leaper":19.0,"Frogtart":19.0,"Peach":19.0,"Sea Breeze":19.0,"Lemon Juice":19.0,"Cranberry":19.0,"Tide Pod":19.0,"Brownie":19.0,"Banana Pox":19.0}

UPDATED_FLOOR_PRICES = {'Plush Pepe':1200.0,'Neko Helmet':15.0,'Sharp Tongue':17.0,"Durov's Cap":251.0,'Voodoo Doll':9.4,'Vintage Cigar':19.7,'Astral Shard':50.0,'Scared Cat':22.0,'Swiss Watch':18.6,'Perfume Bottle':38.3,'Precious Peach':100.0,'Toy Bear':16.3,'Genie Lamp':19.3,'Loot Bag':25.0,'Kissed Frog':14.8, # Base Kissed Frog price, if ever needed standalone
'Electric Skull':10.9,'Diamond Ring':8.06,'Mini Oscar':40.5,'Party Sparkler':2.0,'Homemade Cake':2.0,'Cookie Heart':1.8,'Jack-in-the-box':2.0,'Skull Flower':3.4,'Lol Pop':1.4,'Hynpo Lollipop':1.4,'Desk Calendar':1.4,'B-Day Candle':1.4,'Record Player':4.0,'Jelly Bunny':3.6,'Tama Gadget':4.0,'Snow Globe':4.0,'Eternal Rose':11.0,'Love Potion':5.4,'Top Hat':6.0}

# Populate UPDATED_FLOOR_PRICES with Kissed Frog models including percentage in name
for name_with_percent in KISSED_FROG_MODELS_WITH_PERCENT_PROB.keys():
    model_name_only = strip_percentage_from_name(name_with_percent)
    UPDATED_FLOOR_PRICES[name_with_percent] = KISSED_FROG_VARIANT_BASE_FLOORS.get(model_name_only, 1.0) # Default to 1 if somehow missing

def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    # Handle TON prize first
    if "TON" in name_str.upper() and ("PRIZE" in name_str.upper() or name_str.replace('.', '', 1).replace(' TON', '').strip().isdigit()):
        return TON_PRIZE_IMAGE_DEFAULT
    
    name_for_lookup = strip_percentage_from_name(name_str) # Strip " (X%)" for lookups

    gift_id = GIFT_NAME_TO_ID_MAP_PY.get(name_for_lookup)
    if gift_id:
        return f"https://cdn.changes.tg/gifts/originals/{gift_id}/Original.png"
    
    # Check if it's a Kissed Frog model by checking if the stripped name is in KISSED_FROG_VARIANT_BASE_FLOORS
    if name_for_lookup in KISSED_FROG_VARIANT_BASE_FLOORS: 
        # The base gift_name for CDN URL is "Kissed Frog"
        return f"https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/{name_for_lookup.replace(' ', '%20')}.png"

    # Fallback for other items (name_for_lookup is same as name_str if not a frog model with %)
    if name_for_lookup == "Durov's Cap": return "Durov's-Cap.png" 
    if name_for_lookup == "Vintage Cigar": return "Vintage-Cigar.png"
    name_str_rep = name_for_lookup.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'
    
    cleaned = re.sub(r'\s+', '-', name_for_lookup.replace('&', 'and').replace("'", ""))
    filename = re.sub(r'-+', '-', cleaned)
    if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
        filename += '.png'
    return filename # This will form a relative path if IMAGE_BASE_URL is used in frontend

# Prepare Kissed Frog prize pool for the case
finalKissedFrogPrizesWithConsolation_Python = []
total_kf_prob = 0
for name_with_percent, prob in KISSED_FROG_MODELS_WITH_PERCENT_PROB.items():
    finalKissedFrogPrizesWithConsolation_Python.append({
        'name': name_with_percent, # e.g., "Happy Pepe (0.5%)"
        'probability': prob
    })
    total_kf_prob += prob

# Normalize if sum of probabilities is not exactly 1 (e.g., due to float precision)
if total_kf_prob > 0 and abs(total_kf_prob - 1.0) > 0.0001:
    logger.warning(f"Kissed Frog model probabilities sum to {total_kf_prob}, normalizing.")
    for prize in finalKissedFrogPrizesWithConsolation_Python:
        prize['probability'] = prize['probability'] / total_kf_prob
# Add a small chance for a very common item if total prob is still slightly less than 1
current_sum_kf_probs = sum(p['probability'] for p in finalKissedFrogPrizesWithConsolation_Python)
if current_sum_kf_probs < 0.9999:
    remaining_prob_for_consolation = 1.0 - current_sum_kf_probs
    finalKissedFrogPrizesWithConsolation_Python.append({'name': "Desk Calendar", 'probability': remaining_prob_for_consolation})
    logger.info(f"Added 'Desk Calendar' with {remaining_prob_for_consolation*100:.4f}% as Kissed Frog consolation prize.")


cases_data_backend_with_fixed_prices=[
    {'id':'lolpop','name':'Lol Pop Stash','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Lol-Pop.jpg','priceTON':1.5,'prizes':[{'name':'Plush Pepe','probability':0.00005},{'name':'Neko Helmet','probability':0.0015},{'name':'Party Sparkler','probability':0.115},{'name':'Homemade Cake','probability':0.115},{'name':'Cookie Heart','probability':0.115},{'name':'Jack-in-the-box','probability':0.08},{'name':'Skull Flower','probability':0.035},{'name':'Lol Pop','probability':0.22},{'name':'Hynpo Lollipop','probability':0.21845},{'name':'Desk Calendar','probability':0.05},{'name':'B-Day Candle','probability':0.05}]},
    {'id':'recordplayer','name':'Record Player Vault','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Record-Player.jpg','priceTON':6.0,'prizes':[{'name':'Plush Pepe','probability':0.00015},{'name':'Record Player','probability':0.24},{'name':'Lol Pop','probability':0.15},{'name':'Hynpo Lollipop','probability':0.15},{'name':'Party Sparkler','probability':0.13},{'name':'Skull Flower','probability':0.1},{'name':'Jelly Bunny','probability':0.09985},{'name':'Tama Gadget','probability':0.07},{'name':'Snow Globe','probability':0.06}]},
    {'id':'swisswatch','name':'Swiss Watch Box','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Swiss-Watch.jpg','priceTON':10.0,'prizes':[{'name':'Plush Pepe','probability':0.0002},{'name':'Swiss Watch','probability':0.032},{'name':'Neko Helmet','probability':0.045},{'name':'Eternal Rose','probability':0.06},{'name':'Electric Skull','probability':0.08},{'name':'Diamond Ring','probability':0.1},{'name':'Record Player','probability':0.16},{'name':'Love Potion','probability':0.16},{'name':'Top Hat','probability':0.1728},{'name':'Voodoo Doll','probability':0.19}]},
    {'id':'kissedfrog','name':'Kissed Frog Pond','priceTON':20.0,'imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Kissed-Frog.jpg','prizes':finalKissedFrogPrizesWithConsolation_Python}, # Uses the new list
    {'id':'perfumebottle','name':'Perfume Chest','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Perfume-Bottle.jpg','priceTON':20.0,'prizes':[{'name':'Plush Pepe','probability':0.0004},{'name':'Perfume Bottle','probability':0.02},{'name':'Sharp Tongue','probability':0.035},{'name':'Loot Bag','probability':0.05},{'name':'Swiss Watch','probability':0.06},{'name':'Neko Helmet','probability':0.08},{'name':'Genie Lamp','probability':0.11},{'name':'Kissed Frog','probability':0.15},{'name':'Electric Skull','probability':0.2},{'name':'Diamond Ring','probability':0.2946}]}, # Kissed Frog here refers to the generic one
    {'id':'vintagecigar','name':'Vintage Cigar Safe','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Vintage-Cigar.jpg','priceTON':40.0,'prizes':[{'name':'Plush Pepe','probability':0.0008},{'name':'Perfume Bottle','probability':0.025},{'name':'Vintage Cigar','probability':0.03},{'name':'Swiss Watch','probability':0.04},{'name':'Neko Helmet','probability':0.06},{'name':'Sharp Tongue','probability':0.08},{'name':'Genie Lamp','probability':0.1},{'name':'Mini Oscar','probability':0.07},{'name':'Scared Cat','probability':0.2},{'name':'Toy Bear','probability':0.3942}]},
    {'id':'astralshard','name':'Astral Shard Relic','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Astral-Shard.jpg','priceTON':100.0,'prizes':[{'name':'Plush Pepe','probability':0.0015},{'name':'Durov\'s Cap','probability':0.01},{'name':'Astral Shard','probability':0.025},{'name':'Precious Peach','probability':0.025},{'name':'Vintage Cigar','probability':0.04},{'name':'Perfume Bottle','probability':0.05},{'name':'Swiss Watch','probability':0.07},{'name':'Neko Helmet','probability':0.09},{'name':'Mini Oscar','probability':0.06},{'name':'Scared Cat','probability':0.15},{'name':'Loot Bag','probability':0.2},{'name':'Toy Bear','probability':0.2785}]},
    {'id':'plushpepe','name':'Plush Pepe Hoard','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Plush-Pepe.jpg','priceTON':200.0,'prizes':[{'name':'Plush Pepe','probability':0.045},{'name':'Durov\'s Cap','probability':0.2},{'name':'Astral Shard','probability':0.755}]}
]

for case_data_item in cases_data_backend_with_fixed_prices:
    total_prob = sum(p['probability'] for p in case_data_item['prizes'])
    if total_prob > 0 and abs(total_prob - 1.0) > 0.0001: 
        for prize_item in case_data_item['prizes']:
            prize_item['probability'] = prize_item['probability'] / total_prob

cases_data_backend = []
for case_template in cases_data_backend_with_fixed_prices:
    processed_case = {**case_template}
    if 'imageFilename' not in processed_case or not processed_case['imageFilename'].startswith('http'):
        processed_case['imageFilename'] = generate_image_filename_from_name(processed_case.get('imageFilename', processed_case['name']))
    full_prizes = []
    for prize_stub in processed_case['prizes']:
        prize_name = prize_stub['name'] # This name now includes " (X%)" for frogs
        image_fn_prize = generate_image_filename_from_name(prize_name)
        full_prizes.append({
            'name': prize_name, 
            'imageFilename': image_fn_prize, 
            'floorPrice': UPDATED_FLOOR_PRICES.get(prize_name, 0.1), # Default floor price if somehow missing
            'probability': prize_stub['probability']
        })
    processed_case['prizes'] = full_prizes
    cases_data_backend.append(processed_case)

# Slots are removed, so no slots_data_backend or related prize pools

def populate_initial_data():
    db = SessionLocal()
    try:
        for nft_name_with_percent, floor_price in UPDATED_FLOOR_PRICES.items():
            nft_exists = db.query(NFT).filter(NFT.name == nft_name_with_percent).first()
            img_filename_or_url = generate_image_filename_from_name(nft_name_with_percent)
            
            base_gift = None
            model_only = None
            if strip_percentage_from_name(nft_name_with_percent) in KISSED_FROG_VARIANT_BASE_FLOORS: # Is it a frog model?
                base_gift = "Kissed Frog"
                model_only = strip_percentage_from_name(nft_name_with_percent)
            elif nft_name_with_percent == "Kissed Frog": # The generic Kissed Frog entry
                 base_gift = "Kissed Frog"
                 model_only = None


            if not nft_exists:
                db.add(NFT(name=nft_name_with_percent, image_filename=img_filename_or_url, floor_price=floor_price, base_gift_name=base_gift, model_name_only=model_only))
            elif nft_exists.floor_price != floor_price or nft_exists.image_filename != img_filename_or_url or nft_exists.base_gift_name != base_gift or nft_exists.model_name_only != model_only :
                nft_exists.floor_price = floor_price
                nft_exists.image_filename = img_filename_or_url
                nft_exists.base_gift_name = base_gift
                nft_exists.model_name_only = model_only
        db.commit()
        logger.info("Initial NFT data populated/updated successfully.")
    except Exception as e:
        db.rollback(); logger.error(f"Error populating initial NFT data: {e}", exc_info=True)
    finally: db.close()

def calculate_and_log_rtp(): # Simplified as slots are removed
    logger.info("--- RTP Calculations (Based on Current Fixed Prices & Probabilities) ---")
    overall_total_ev_weighted_by_price = Decimal('0'); overall_total_cost_squared = Decimal('0')
    all_games_data = cases_data_backend # Only cases now
    for game_data in all_games_data:
        game_id = game_data['id']; game_name = game_data['name']; price = Decimal(str(game_data['priceTON'])); ev = Decimal('0')
        if 'prizes' in game_data: 
            for prize in game_data['prizes']:
                prize_value = Decimal(str(UPDATED_FLOOR_PRICES.get(prize['name'], 0)))
                ev += prize_value * Decimal(str(prize['probability']))
        rtp = (ev / price) * 100 if price > 0 else Decimal('0'); dev_cut = 100 - rtp if price > 0 else Decimal('0')
        logger.info(f"Game: {game_name:<35} | Price: {price:>6.2f} TON | Est.EV: {ev:>7.3f} | Est.RTP: {rtp:>6.2f}% | Est.DevCut: {dev_cut:>6.2f}%")
        if price > 0: overall_total_ev_weighted_by_price += ev * price; overall_total_cost_squared += price * price
    if overall_total_cost_squared > 0:
        weighted_avg_rtp = (overall_total_ev_weighted_by_price / overall_total_cost_squared) * 100
        logger.info(f"--- Approx. Weighted Avg RTP (by price, for cases): {weighted_avg_rtp:.2f}% ---")
    else: logger.info("--- No priced games for overall RTP calculation. ---")


def initial_setup_and_logging():
    populate_initial_data()
    db = SessionLocal()
    try:
        if not db.query(PromoCode).filter(PromoCode.code_text == 'Grachev').first():
            db.add(PromoCode(code_text='Grachev', activations_left=10, ton_amount=100.0))
            db.commit()
    except Exception as e: db.rollback(); logger.error(f"Error seeding Grachev promocode: {e}")
    finally: db.close()
    calculate_and_log_rtp()

initial_setup_and_logging()
DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR"
DEPOSIT_COMMENT = "cpd7r07ud3s" 
PENDING_DEPOSIT_EXPIRY_MINUTES = 30 
app = Flask(__name__)
PROD_ORIGIN = "https://vasiliy-katsyka.github.io"; NULL_ORIGIN = "null" 
LOCAL_DEV_ORIGINS = ["http://localhost:5500","http://127.0.0.1:5500","http://localhost:8000","http://127.0.0.1:8000",]
final_allowed_origins = list(set([PROD_ORIGIN, NULL_ORIGIN] + LOCAL_DEV_ORIGINS))
CORS(app, resources={r"/api/*": {"origins": final_allowed_origins}})
bot = telebot.TeleBot(BOT_TOKEN)
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

@app.route(WEBHOOK_PATH, methods=['POST'])
def webhook_handler():
    if flask_request.headers.get('content-type') == 'application/json':
        json_string = flask_request.get_data().decode('utf-8')
        logger.debug(f"Webhook received JSON: {json_string[:500]}")
        try: update = telebot.types.Update.de_json(json_string); bot.process_new_updates([update]); return '', 200
        except Exception as e: logger.error(f"Error processing update from webhook: {e}", exc_info=True); return 'Error processing update', 500
    else: logger.warning(f"Webhook received non-JSON request from {flask_request.remote_addr}"); return 'Invalid content type', 403

def get_db(): db = SessionLocal();_ = None; yield db; db.close()

def validate_init_data(init_data_str: str, bot_token_val: str) -> dict | None:
    logger.debug(f"Attempting to validate initData: {init_data_str[:200]}...")
    try:
        if not init_data_str: logger.warning("validate_init_data: init_data_str is empty."); return None
        parsed_data = dict(parse_qs(init_data_str))
        required_keys = ['hash', 'user', 'auth_date']
        if any(k not in parsed_data for k in required_keys): logger.warning(f"validate_init_data: Missing keys. Parsed: {list(parsed_data.keys())}"); return None
        hash_received = parsed_data.pop('hash')[0]; auth_date_ts = int(parsed_data['auth_date'][0])
        if (int(dt.now(timezone.utc).timestamp()) - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS: logger.warning(f"validate_init_data: auth_date expired."); return None
        data_check_string = "\n".join([f"{k}={parsed_data[k][0]}" for k in sorted(parsed_data.keys())])
        secret_key = hmac.new("WebAppData".encode(), bot_token_val.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash_hex == hash_received:
            try: user_info_dict = json.loads(unquote(parsed_data['user'][0]))
            except json.JSONDecodeError as je: logger.error(f"validate_init_data: Failed to parse user JSON. Error: {je}"); return None
            if 'id' not in user_info_dict: logger.warning(f"validate_init_data: 'id' not in user_info_dict."); return None
            user_info_dict['id'] = int(user_info_dict['id']); logger.info(f"validate_init_data: Auth successful for user ID: {user_info_dict.get('id')}.")
            return user_info_dict
        else: logger.warning(f"validate_init_data: Hash mismatch."); return None
    except Exception as e_validate: logger.error(f"validate_init_data: General exception: {e_validate}", exc_info=True); return None

@app.route('/')
def index_route(): return "Pusik Gifts App is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user: user = User(id=uid,username=auth.get("username"),first_name=auth.get("first_name"),last_name=auth.get("last_name"),referral_code=f"ref_{uid}_{random.randint(1000,9999)}"); db.add(user); db.commit(); db.refresh(user)
        inv = [{"id":i.id,"name":i.nft.name if i.nft else i.item_name_override,"imageFilename":i.nft.image_filename if i.nft else i.item_image_override,"floorPrice":i.nft.floor_price if i.nft else i.current_value,"currentValue":i.current_value,"upgradeMultiplier":i.upgrade_multiplier,"variant":i.variant,"is_ton_prize":i.is_ton_prize,"obtained_at":i.obtained_at.isoformat() if i.obtained_at else None} for i in user.inventory]
        refs = db.query(User).filter(User.referred_by_id == uid).count()
        return jsonify({"id":user.id,"username":user.username,"first_name":user.first_name,"last_name":user.last_name,"tonBalance":user.ton_balance,"starBalance":user.star_balance,"inventory":inv,"referralCode":user.referral_code,"referralEarningsPending":user.referral_earnings_pending,"total_won_ton":user.total_won_ton,"invited_friends_count":refs})
    except Exception as e: logger.error(f"Error in get_user_data for {uid}: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); cid = data.get('case_id'); multiplier = int(data.get('multiplier', 1))
    if not cid or multiplier not in [1,2,3]: return jsonify({"error": "Invalid params"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first() # Lock user row
        if not user: return jsonify({"error": "User not found"}), 404
        tcase = next((c for c in cases_data_backend if c['id'] == cid), None)
        if not tcase: return jsonify({"error": "Case not found"}), 404
        base_cost = Decimal(str(tcase['priceTON'])); total_cost = base_cost * Decimal(multiplier)
        if Decimal(str(user.ton_balance)) < total_cost: return jsonify({"error": f"Not enough TON. Need {total_cost:.2f}"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - total_cost)
        prizes_in_case = tcase['prizes']; won_prizes_list = []; total_value_this_spin = Decimal('0')
        for _i_loop_multiplier in range(multiplier): # Use a named loop variable
            rv = random.random(); cprob = 0; chosen_prize_info = None
            for p_info in prizes_in_case:
                cprob += p_info['probability']
                if rv <= cprob:
                    chosen_prize_info = p_info
                    break # Exit inner loop once prize is chosen
            if not chosen_prize_info and prizes_in_case: # Fallback if not chosen due to float precision, and list is not empty
                chosen_prize_info = random.choice(prizes_in_case)
            
            if not chosen_prize_info: # If still not chosen (e.g. empty prize list)
                logger.error(f"Could not choose prize for case {cid} in multiplier loop {_i_loop_multiplier+1}")
                continue # Skip to next iteration of multiplier loop
            
            # chosen_prize_info['name'] now includes " (X%)" for Kissed Frog models
            dbnft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
            if not dbnft: logger.error(f"NFT {chosen_prize_info['name']} missing from DB during case open!"); continue 
            
            variant_name_to_store_in_inventory_variant_field = None
            if dbnft.base_gift_name == "Kissed Frog" and dbnft.model_name_only: # It's a specific frog model
                variant_name_to_store_in_inventory_variant_field = dbnft.model_name_only # Store "Happy Pepe"

            actual_val = Decimal(str(dbnft.floor_price))
            total_value_this_spin += actual_val
            item = InventoryItem(user_id=uid,nft_id=dbnft.id,current_value=float(actual_val.quantize(Decimal('0.01'))),variant=variant_name_to_store_in_inventory_variant_field,is_ton_prize=False)
            db.add(item); db.flush() 
            won_prizes_list.append({"id":item.id,"name":dbnft.name,"imageFilename":dbnft.image_filename,"floorPrice":float(dbnft.floor_price),"currentValue":item.current_value,"variant":item.variant,"is_ton_prize":False})
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status":"success","won_prizes":won_prizes_list,"new_balance_ton":user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Error in open_case: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
    finally: db.close()

# REMOVED: /api/spin_slot endpoint

@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id'); mult_str = data.get('multiplier_str')
    if not all([iid, mult_str]): return jsonify({"error": "Missing params"}), 400
    try: mult = Decimal(mult_str); iid_int = int(iid)
    except (InvalidOperation, ValueError): return jsonify({"error": "Invalid data"}), 400
    chances = {Decimal("1.5"):50,Decimal("2.0"):35,Decimal("3.0"):25,Decimal("5.0"):15,Decimal("10.0"):8,Decimal("20.0"):3}
    if mult not in chances: return jsonify({"error": "Invalid multiplier"}), 400
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_int, InventoryItem.user_id == uid).options(joinedload(InventoryItem.nft)).with_for_update().first()
        if not item or item.is_ton_prize: return jsonify({"error": "Item not found or not upgradable"}), 404
        user = db.query(User).filter(User.id == uid).with_for_update().first() 
        if not user: return jsonify({"error": "User not found for upgrade"}), 404 # Should not happen if item exists
        if random.uniform(0,100) < chances[mult]:
            orig_val = Decimal(str(item.current_value)); new_val = (orig_val * mult).quantize(Decimal('0.01'), ROUND_HALF_UP)
            increase = new_val - orig_val; item.current_value = float(new_val)
            item.upgrade_multiplier = float(Decimal(str(item.upgrade_multiplier)) * mult)
            user.total_won_ton = float(Decimal(str(user.total_won_ton)) + increase)
            db.commit()
            return jsonify({"status":"success","message":f"Upgraded! New value: {new_val:.2f} TON","item":{"id":item.id,"currentValue":item.current_value,"name":item.nft.name,"imageFilename":item.nft.image_filename,"upgradeMultiplier":item.upgrade_multiplier,"variant":item.variant }})
        else:
            name_lost = item.nft.name; val_lost = Decimal(str(item.current_value))
            user.total_won_ton = float(Decimal(str(user.total_won_ton)) - val_lost) 
            db.delete(item); db.commit()
            return jsonify({"status":"failed","message":f"Upgrade failed! Lost {name_lost}.","item_lost":True})
    except Exception as e: db.rollback(); logger.error(f"Error in upgrade_item: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid_convert = data.get('inventory_item_id')
    if not iid_convert: return jsonify({"error": "ID required"}), 400
    try: iid_convert_int = int(iid_convert)
    except ValueError: return jsonify({"error": "Invalid ID"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_convert_int, InventoryItem.user_id == uid).options(joinedload(InventoryItem.nft)).with_for_update().first()
        if not user or not item: return jsonify({"error": "User or item not found"}), 404
        if item.is_ton_prize: return jsonify({"error": "Cannot convert TON prize."}), 400
        val = Decimal(str(item.current_value)); user.ton_balance = float(Decimal(str(user.ton_balance)) + val)
        item_name_converted = item.nft.name; db.delete(item); db.commit()
        return jsonify({"status":"success","message":f"{item_name_converted} sold for {val:.2f} TON.","new_balance_ton":user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Error in convert_to_ton: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"error": "User not found"}), 404
        items_to_sell = [item_obj for item_obj in user.inventory if not item_obj.is_ton_prize]
        if not items_to_sell: return jsonify({"status":"no_items","message":"No sellable items."})
        total_val = sum(Decimal(str(i_sell.current_value)) for i_sell in items_to_sell)
        user.ton_balance = float(Decimal(str(user.ton_balance)) + total_val)
        for i_del in items_to_sell: db.delete(i_del)
        db.commit()
        return jsonify({"status":"success","message":f"All {len(items_to_sell)} sellable items sold for {total_val:.2f} TON.","new_balance_ton":user.ton_balance})
    except Exception as e: db.rollback(); logger.error(f"Error in sell_all_items: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
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
        user = db.query(User).filter(User.id == uid).first()
        if not user: return jsonify({"error": "User not found"}), 404
        if db.query(PendingDeposit).filter(PendingDeposit.user_id == uid, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first():
            return jsonify({"error": "You have an active deposit. Please wait."}), 409 # HTTP 409 Conflict
        nano_part = random.randint(10000, 999999); 
        final_nano_amt = int(orig_amt * 1e9) + nano_part
        pdep = PendingDeposit(user_id=uid,original_amount_ton=orig_amt,unique_identifier_nano_ton=nano_part,final_amount_nano_ton=final_nano_amt,expected_comment=DEPOSIT_COMMENT,expires_at=dt.now(timezone.utc)+timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
        db.add(pdep); db.commit(); db.refresh(pdep)
        disp_amt = f"{final_nano_amt / 1e9:.9f}".rstrip('0').rstrip('.') 
        return jsonify({"status":"success","pending_deposit_id":pdep.id,"recipient_address":DEPOSIT_RECIPIENT_ADDRESS_RAW,"amount_to_send":disp_amt,"final_amount_nano_ton":final_nano_amt,"comment":DEPOSIT_COMMENT,"expires_at":pdep.expires_at.isoformat()})
    except Exception as e: db.rollback(); logger.error(f"Error in initiate_deposit: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
    finally: db.close()

async def check_blockchain_for_deposit(pdep: PendingDeposit, db_sess):
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
                            if not usr: pdep.status = 'failed_user_not_found'; db_sess.commit(); return {"status":"error","message":"User for deposit not found."}
                            usr.ton_balance = float(Decimal(str(usr.ton_balance)) + Decimal(str(pdep.original_amount_ton)))
                            if usr.referred_by_id:
                                referrer = db_sess.query(User).filter(User.id == usr.referred_by_id).with_for_update().first()
                                if referrer:
                                    referral_bonus = (Decimal(str(pdep.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                    referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + referral_bonus)
                            pdep.status = 'completed'; db_sess.commit()
                            return {"status":"success","message":"Deposit confirmed!","new_balance_ton":usr.ton_balance}
                    except Exception as e_cmt: logger.debug(f"Comment parse error for tx {tx.hash}: {e_cmt}")
        if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending':
            pdep.status = 'expired'; db_sess.commit()
            return {"status":"expired","message":"Deposit expired."}
        return {"status":"pending","message":"Transaction not confirmed yet."}
    except Exception as e: logger.error(f"Blockchain check error: {e}", exc_info=True); return {"status":"error","message":"Error checking blockchain."}
    finally: 
        if prov: await prov.close_all()

@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); pid = data.get('pending_deposit_id')
    if not pid: return jsonify({"error": "Pending deposit ID required"}), 400
    db = next(get_db())
    try:
        pdep = db.query(PendingDeposit).filter(PendingDeposit.id == pid, PendingDeposit.user_id == uid).with_for_update().first()
        if not pdep: return jsonify({"error": "Pending deposit not found"}), 404
        if pdep.status == 'completed': usr = db.query(User).filter(User.id == uid).first(); return jsonify({"status":"success","message":"Deposit already confirmed.","new_balance_ton":usr.ton_balance if usr else 0})
        if pdep.status == 'expired' or (pdep.status == 'pending' and pdep.expires_at <= dt.now(timezone.utc)):
            if pdep.status == 'pending': pdep.status = 'expired'; db.commit() 
            return jsonify({"status":"expired","message":"Deposit has expired."}), 400
        
        result = {}
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop) # Always use new loop for safety in sync context
        try: result = loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
        finally: loop.close()
        return jsonify(result)

    except Exception as e: db.rollback(); logger.error(f"Outer error in verify_deposit: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    try:
        leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        return jsonify([{"rank":r_idx+1,"name":u_leader.first_name or u_leader.username or f"User_{str(u_leader.id)[:6]}","avatarChar":(u_leader.first_name or u_leader.username or "U")[0].upper(),"income":u_leader.total_won_ton,"user_id":u_leader.id} for r_idx, u_leader in enumerate(leaders)])
    except Exception as e: logger.error(f"Error in get_leaderboard: {e}", exc_info=True); return jsonify({"error":"Could not load leaderboard"}), 500
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
            return jsonify({"status":"success","message":f"{withdrawn_amount:.2f} TON withdrawn.","new_balance_ton":user.ton_balance,"new_referral_earnings_pending":0.0})
        else: return jsonify({"status":"no_earnings","message":"No earnings to withdraw."})
    except Exception as e: db.rollback(); logger.error(f"Error withdrawing ref earnings: {e}", exc_info=True); return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); code_txt = data.get('promocode_text', "").strip()
    if not code_txt: return jsonify({"status":"error","message":"Promocode cannot be empty."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user: return jsonify({"status":"error","message":"User not found."}), 404
        promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).with_for_update().first()
        if not promo: return jsonify({"status":"error","message":"Invalid promocode."}), 404
        if promo.activations_left != -1 and promo.activations_left <= 0 : return jsonify({"status":"error","message":"Promocode has no activations left."}), 400
        if db.query(UserPromoCodeRedemption).filter(UserPromoCodeRedemption.user_id == user.id, UserPromoCodeRedemption.promo_code_id == promo.id).first():
            return jsonify({"status":"error","message":"You have already redeemed this promocode."}), 400
        if promo.activations_left != -1: promo.activations_left -= 1
        user.ton_balance = float(Decimal(str(user.ton_balance)) + Decimal(str(promo.ton_amount)))
        db.add(UserPromoCodeRedemption(user_id=user.id, promo_code_id=promo.id)); db.commit()
        return jsonify({"status":"success","message":f"Promocode '{code_txt}' redeemed! +{promo.ton_amount:.2f} TON.","new_balance_ton":user.ton_balance})
    except IntegrityError as ie: db.rollback(); logger.error(f"IntegrityError redeeming promocode: {ie}", exc_info=True); return jsonify({"status":"error","message":"Promocode redemption failed. Try again."}), 500
    except Exception as e: db.rollback(); logger.error(f"Error redeeming promocode: {e}", exc_info=True); return jsonify({"status":"error","message":"Server error during promocode redemption."}), 500
    finally: db.close()

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST'])
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"status":"error","message":"Auth failed"}), 401
    player_user_id = auth_user_data["id"]
    if not TONNEL_SENDER_INIT_DATA: logger.error("Tonnel: TONNEL_SENDER_INIT_DATA not set."); return jsonify({"status":"error","message":"Withdrawal service unavailable."}), 500
    db = next(get_db())
    try:
        item_to_withdraw = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == player_user_id).options(joinedload(InventoryItem.nft)).with_for_update().first()
        if not item_to_withdraw or item_to_withdraw.is_ton_prize: return jsonify({"status":"error","message":"Item not found or not withdrawable."}), 404
        if not item_to_withdraw.nft: logger.error(f"Withdrawal failed: InventoryItem ID {item_to_withdraw.id} has no linked NFT."); return jsonify({"status":"error","message":"Item data corrupted."}), 500

        base_gift_name_for_tonnel = item_to_withdraw.nft.base_gift_name or item_to_withdraw.nft.name # Fallback to full name if base_gift_name is null
        model_name_for_tonnel = item_to_withdraw.nft.model_name_only # This is "Model Name" (e.g. Happy Pepe) or None

        # If base_gift_name was not specifically "Kissed Frog" (e.g. for older items or non-frogs)
        # but the nft.name indicates it IS a frog model, we correct it.
        if base_gift_name_for_tonnel != "Kissed Frog" and model_name_for_tonnel and item_to_withdraw.nft.name.startswith(model_name_for_tonnel):
             # This means nft.name was like "Happy Pepe (0.5%)" and model_name_only was "Happy Pepe"
             # but base_gift_name might have been null or also "Happy Pepe (0.5%)"
             base_gift_name_for_tonnel = "Kissed Frog" # Force base name for Tonnel API
             logger.info(f"Corrected base_gift_name to 'Kissed Frog' for model '{model_name_for_tonnel}' during withdrawal.")


        tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        tonnel_result = {}
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        try:
            tonnel_result = loop.run_until_complete(
                tonnel_client.send_gift_to_user(
                    gift_item_name=base_gift_name_for_tonnel, 
                    receiver_telegram_id=player_user_id,
                    gift_model_name=model_name_for_tonnel 
                )
            )
        finally: loop.close()
            
        if tonnel_result and tonnel_result.get("status") == "success":
            val_deducted = Decimal(str(item_to_withdraw.current_value))
            player = db.query(User).filter(User.id == player_user_id).with_for_update().first()
            if player: player.total_won_ton = float(max(Decimal('0'), Decimal(str(player.total_won_ton)) - val_deducted)) 
            db.delete(item_to_withdraw); db.commit()
            return jsonify({"status":"success","message":f"Gift '{item_to_withdraw.nft.name}' sent! {tonnel_result.get('message', '')}","details":tonnel_result.get("details")})
        else:
            db.rollback(); return jsonify({"status":"error","message":f"Tonnel withdrawal failed: {tonnel_result.get('message', 'Tonnel API error')}"}), 500
    except Exception as e: db.rollback(); logger.error(f"Tonnel withdrawal wrapper exception: {e}", exc_info=True); return jsonify({"status":"error","message":"Unexpected error during withdrawal."}), 500
    finally: db.close()

# --- Bot Handlers ---
@bot.message_handler(commands=['start'])
# --- Bot Handlers ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    db = next(get_db())
    try:
        user_id = message.chat.id; tg_user_obj = message.from_user
        user = db.query(User).filter(User.id == user_id).first(); created_now = False
        if not user:
            created_now = True
            user = User(id=user_id,username=tg_user_obj.username,first_name=tg_user_obj.first_name,last_name=tg_user_obj.last_name,referral_code=f"ref_{user_id}_{random.randint(1000,9999)}")
            db.add(user)
        try: 
            command_parts = message.text.split(' ')
            if len(command_parts) > 1 and command_parts[1].startswith('startapp='):
                start_param_value = command_parts[1].split('=')[1]
                if start_param_value.startswith('ref_'):
                    referrer_code = start_param_value
                    if (created_now or not user.referred_by_id) and user.referral_code != referrer_code : 
                        referrer = db.query(User).filter(User.referral_code == referrer_code, User.id != user.id).first()
                        if referrer:
                            user.referred_by_id = referrer.id
                            try: # Nested try for sending message to referrer
                                bot.send_message(referrer.id, f"🎉 Friend {user.first_name or user.username or user.id} joined via your link!")
                            except Exception as e_notify: # More specific exception handling
                                logger.warning(f"Failed to notify referrer {referrer.id}: {e_notify}")
        except Exception as e_param: # Catch errors from processing start_param
            logger.error(f"Error processing start param for {user_id}: {e_param}")

        updated_fields = False 
        if user.username != tg_user_obj.username: user.username = tg_user_obj.username; updated_fields = True
        if user.first_name != tg_user_obj.first_name: user.first_name = tg_user_obj.first_name; updated_fields = True
        if user.last_name != tg_user_obj.last_name: user.last_name = tg_user_obj.last_name; updated_fields = True
        
        if created_now or updated_fields or (user.referred_by_id and created_now): 
            try: # Nested try for commit
                db.commit()
            except Exception as e_commit: # More specific exception handling
                db.rollback()
                logger.error(f"Error committing user {user_id}: {e_commit}")
            
        markup = types.InlineKeyboardMarkup()
        web_app_info = types.WebAppInfo(url=WEBAPP_URL)
        app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app_info)
        markup.add(app_button)
        bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже, чтобы начать!", reply_markup=markup)
    except Exception as e_start: # General catch for the whole function
        logger.error(f"General error in /start for chat {message.chat.id}: {e_start}", exc_info=True)
        try:
            bot.send_message(message.chat.id, "An error occurred. Try again.")
        except Exception as e_send_err:
            logger.error(f"Failed to send error message to chat {message.chat.id}: {e_send_err}")
    finally:
        db.close()

@bot.message_handler(func=lambda message: True)
def echo_all(message): bot.reply_to(message, "Нажмите /start, чтобы открыть Pusik Gifts.")

if __name__ == '__main__':
    if BOT_TOKEN and WEBHOOK_URL_BASE and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        logger.info("Attempting to set webhook...")
        time.sleep(2) 
        try:
            bot.remove_webhook()
            logger.info("Existing webhook removed (if any).")
        except Exception as e_rem_wh:
            logger.warning(f"Could not remove existing webhook: {e_rem_wh}")
        
        time.sleep(0.5) # Brief pause
        
        webhook_url_full = f"{WEBHOOK_URL_BASE.rstrip('/')}{WEBHOOK_PATH}"
        logger.info(f"Setting webhook for bot to: {webhook_url_full}")
        
        try:
            if bot.set_webhook(url=webhook_url_full):
                logger.info("Webhook set successfully.")
            else:
                logger.error("FAILED to set webhook. Check telebot logs for details.")
                logger.info("Ensure your WEBHOOK_URL_BASE is publicly accessible (HTTPS) and correct.")
        except Exception as e_set_wh:
            logger.error(f"Exception while setting webhook: {e_set_wh}", exc_info=True)

    elif not WEBHOOK_URL_BASE and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        logger.warning("WEBHOOK_URL_BASE environment variable not set. Webhook NOT configured.")
        logger.warning("Bot will not receive updates from Telegram via webhook.")
        try:
            logger.info("Removing any existing webhook to prevent issues...")
            bot.remove_webhook()
        except Exception as e_rem_fallback:
            logger.warning(f"Could not remove existing webhook during fallback: {e_rem_fallback}")
    
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
