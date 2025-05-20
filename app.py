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
    logger.error("DATABASE_URL not set!")
    exit("DATABASE_URL is not set. Exiting.")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN not set!")
    exit("BOT_TOKEN is not set. Exiting.")
if not TONNEL_SENDER_INIT_DATA:
    logger.warning("TONNEL_SENDER_INIT_DATA not set! Tonnel gift withdrawal will likely fail.")

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
            if method.upper() == "GET": response_obj = await session.get(url, **request_kwargs)
            elif method.upper() == "POST": response_obj = await session.post(url, **request_kwargs)
            elif method.upper() == "OPTIONS": response_obj = await session.options(url, **request_kwargs)
            else: raise ValueError(f"Unsupported HTTP method: {method}")
            if method.upper() == "OPTIONS":
                if 200 <= response_obj.status_code < 300: return {"status": "options_ok"}
                else:
                    err_text_options = await response_obj.text()
                    logger.error(f"Tonnel API OPTIONS {url} failed: {response_obj.status_code}. Resp: {err_text_options[:500]}")
                    response_obj.raise_for_status()
                    return {"status": "error", "message": f"OPTIONS failed: {response_obj.status_code}"}
            response_obj.raise_for_status()
            if response_obj.status_code == 204: return None
            content_type = response_obj.headers.get("Content-Type", "").lower()
            if "application/json" in content_type:
                try: return response_obj.json()
                except json.JSONDecodeError as je_err_inner:
                    err_text_json_decode = await response_obj.text()
                    logger.error(f"Tonnel API JSONDecodeError for {method} {url}: {je_err_inner}. Body: {err_text_json_decode[:500]}")
                    return {"status": "error", "message": "Invalid JSON in response", "raw_text": err_text_json_decode[:500]}
            else:
                if is_initial_get: return {"status": "get_ok_non_json"}
                else:
                    responseText = await response_obj.text()
                    logger.warning(f"Tonnel API {method} {url} - Non-JSON (Type: {content_type}). Text: {responseText[:200]}")
                    return {"status": "error", "message": "Response not JSON", "content_type": content_type, "text_preview": responseText[:200]}
        except RequestsError as re_err:
            logger.error(f"Tonnel API RequestsError ({method} {url}): {re_err}")
            raise
        except json.JSONDecodeError as je_err:
            logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err}")
            raise ValueError(f"Failed to decode JSON from {url}") from je_err
        except Exception as e_gen:
            logger.error(f"Tonnel API general request error ({method} {url}): {type(e_gen).__name__} - {e_gen}")
            raise
    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int):
        if not self.authdata: return {"status": "error", "message": "Tonnel sender not configured."}
        try:
            await self._make_request(method="GET", url="https://marketplace.tonnel.network/", is_initial_get=True)
            filter_str=json.dumps({"price":{"$exists":True},"refunded":{"$ne":True},"buyer":{"$exists":False},"export_at":{"$exists":True},"gift_name":gift_item_name,"asset":"TON"})
            page_gifts_payload={"filter":filter_str,"limit":10,"page":1,"sort":'{"price":1,"gift_id":-1}'}
            pg_headers_options={"Access-Control-Request-Method":"POST","Access-Control-Request-Headers":"content-type","Origin":"https://tonnel-gift.vercel.app","Referer":"https://tonnel-gift.vercel.app/"}
            pg_headers_post={"Content-Type":"application/json","Origin":"https://marketplace.tonnel.network","Referer":"https://marketplace.tonnel.network/"}
            await self._make_request(method="OPTIONS",url="https://gifts2.tonnel.network/api/pageGifts",headers=pg_headers_options)
            gifts_found_response=await self._make_request(method="POST",url="https://gifts2.tonnel.network/api/pageGifts",headers=pg_headers_post,json_payload=page_gifts_payload)
            if not isinstance(gifts_found_response,list): return {"status":"error","message":f"Could not fetch gift list: {gifts_found_response.get('message','API error') if isinstance(gifts_found_response,dict) else 'Format error'}"}
            if not gifts_found_response: return {"status":"error","message":f"No '{gift_item_name}' gifts available on Tonnel."}
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
            logger.error(f"Tonnel error sending gift '{gift_item_name}' to {receiver_telegram_id}: {type(e).__name__} - {e}", exc_info=True)
            return {"status":"error","message":f"Unexpected error during Tonnel withdrawal: {str(e)}"}
        finally: await self._close_session_if_open()

def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if name_str == "Durov's Cap": return "Durov's-Cap.png"
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png"
    if "TON" in name_str and ("prize" in name_str.lower() or name_str.replace('.', '').isdigit()): return "ton-logo.png"
    name_str_rep = name_str.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'
    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    return re.sub(r'-+', '-', cleaned) + '.png'

UPDATED_FLOOR_PRICES = {'Plush Pepe':1200.0,'Neko Helmet':15.0,'Sharp Tongue':17.0,"Durov's Cap":251.0,'Voodoo Doll':9.4,'Vintage Cigar':19.7,'Astral Shard':50.0,'Scared Cat':22.0,'Swiss Watch':18.6,'Perfume Bottle':38.3,'Precious Peach':100.0,'Toy Bear':16.3,'Genie Lamp':19.3,'Loot Bag':25.0,'Kissed Frog':14.8,'Electric Skull':10.9,'Diamond Ring':8.06,'Mini Oscar':40.5,'Party Sparkler':2.0,'Homemade Cake':2.0,'Cookie Heart':1.8,'Jack-in-the-box':2.0,'Skull Flower':3.4,'Lol Pop':1.4,'Hynpo Lollipop':1.4,'Desk Calendar':1.4,'B-Day Candle':1.4,'Record Player':4.0,'Jelly Bunny':3.6,'Tama Gadget':4.0,'Snow Globe':4.0,'Eternal Rose':11.0,'Love Potion':5.4,'Top Hat':6.0}
KISSED_FROG_VARIANT_FLOORS = {"Happy Pepe":500.0,"Tree Frog":150.0,"Brewtoad":150.0,"Puddles":150.0,"Honeyhop":150.0,"Melty Butter":150.0,"Lucifrog":150.0,"Zodiak Croak":150.0,"Count Croakula":150.0,"Lilie Pond":150.0,"Sweet Dream":150.0,"Frogmaid":150.0,"Rocky Hopper":150.0,"Icefrog":45.0,"Lava Leap":45.0,"Toadstool":45.0,"Desert Frog":45.0,"Cupid":45.0,"Hopberry":45.0,"Ms. Toad":45.0,"Trixie":45.0,"Prince Ribbit":45.0,"Pond Fairy":45.0,"Boingo":45.0,"Tesla Frog":45.0,"Starry Night":30.0,"Silver":30.0,"Ectofrog":30.0,"Poison":30.0,"Minty Bloom":30.0,"Sarutoad":30.0,"Void Hopper":30.0,"Ramune":30.0,"Lemon Drop":30.0,"Ectobloom":30.0,"Duskhopper":30.0,"Bronze":30.0,"Lily Pond":19.0,"Toadberry":19.0,"Frogwave":19.0,"Melon":19.0,"Sky Leaper":19.0,"Frogtart":19.0,"Peach":19.0,"Sea Breeze":19.0,"Lemon Juice":19.0,"Cranberry":19.0,"Tide Pod":19.0,"Brownie":19.0,"Banana Pox":19.0}
UPDATED_FLOOR_PRICES.update(KISSED_FROG_VARIANT_FLOORS)

finalKissedFrogPrizesWithConsolation_Python=[{'name':"Happy Pepe",'probability':0.00010},{'name':"Tree Frog",'probability':0.00050},{'name':"Brewtoad",'probability':0.00050},{'name':"Puddles",'probability':0.00050},{'name':"Honeyhop",'probability':0.00050},{'name':"Melty Butter",'probability':0.00050},{'name':"Lucifrog",'probability':0.00050},{'name':"Zodiak Croak",'probability':0.00050},{'name':"Count Croakula",'probability':0.00050},{'name':"Lilie Pond",'probability':0.00050},{'name':"Sweet Dream",'probability':0.00050},{'name':"Frogmaid",'probability':0.00050},{'name':"Rocky Hopper",'probability':0.00050},{'name':"Icefrog",'probability':0.0020},{'name':"Lava Leap",'probability':0.0020},{'name':"Toadstool",'probability':0.0020},{'name':"Desert Frog",'probability':0.0020},{'name':"Cupid",'probability':0.0020},{'name':"Hopberry",'probability':0.0020},{'name':"Ms. Toad",'probability':0.0020},{'name':"Trixie",'probability':0.0020},{'name':"Prince Ribbit",'probability':0.0020},{'name':"Pond Fairy",'probability':0.0020},{'name':"Boingo",'probability':0.0020},{'name':"Tesla Frog",'probability':0.0020},{'name':"Starry Night",'probability':0.0070},{'name':"Silver",'probability':0.0070},{'name':"Ectofrog",'probability':0.0070},{'name':"Poison",'probability':0.0070},{'name':"Minty Bloom",'probability':0.0070},{'name':"Sarutoad",'probability':0.0070},{'name':"Void Hopper",'probability':0.0070},{'name':"Ramune",'probability':0.0070},{'name':"Lemon Drop",'probability':0.0070},{'name':"Ectobloom",'probability':0.0070},{'name':"Duskhopper",'probability':0.0070},{'name':"Bronze",'probability':0.0070},{'name':"Lily Pond",'probability':0.04028},{'name':"Toadberry",'probability':0.04028},{'name':"Frogwave",'probability':0.04028},{'name':"Melon",'probability':0.04028},{'name':"Sky Leaper",'probability':0.04028},{'name':"Frogtart",'probability':0.04028},{'name':"Peach",'probability':0.04028},{'name':"Sea Breeze",'probability':0.04028},{'name':"Lemon Juice",'probability':0.04028},{'name':"Cranberry",'probability':0.04028},{'name':"Tide Pod",'probability':0.04028},{'name':"Brownie",'probability':0.04028},{'name':"Banana Pox",'probability':0.04024}]
currentProbSumForKF_py=sum(p['probability']for p in finalKissedFrogPrizesWithConsolation_Python)
remainingProbForConsolation_py=1.0-currentProbSumForKF_py
if remainingProbForConsolation_py>0.00001:finalKissedFrogPrizesWithConsolation_Python.append({'name':"Desk Calendar",'probability':remainingProbForConsolation_py})

cases_data_backend_with_fixed_prices=[{'id':'lolpop','name':'Lol Pop Stash','priceTON':1.5,'prizes':[{'name':'Plush Pepe','probability':0.00005},{'name':'Neko Helmet','probability':0.0015},{'name':'Party Sparkler','probability':0.115},{'name':'Homemade Cake','probability':0.115},{'name':'Cookie Heart','probability':0.115},{'name':'Jack-in-the-box','probability':0.08},{'name':'Skull Flower','probability':0.035},{'name':'Lol Pop','probability':0.22},{'name':'Hynpo Lollipop','probability':0.21845},{'name':'Desk Calendar','probability':0.05},{'name':'B-Day Candle','probability':0.05}]},{'id':'recordplayer','name':'Record Player Vault','priceTON':6.0,'prizes':[{'name':'Plush Pepe','probability':0.00015},{'name':'Record Player','probability':0.24},{'name':'Lol Pop','probability':0.15},{'name':'Hynpo Lollipop','probability':0.15},{'name':'Party Sparkler','probability':0.13},{'name':'Skull Flower','probability':0.1},{'name':'Jelly Bunny','probability':0.09985},{'name':'Tama Gadget','probability':0.07},{'name':'Snow Globe','probability':0.06}]},{'id':'swisswatch','name':'Swiss Watch Box','priceTON':10.0,'prizes':[{'name':'Plush Pepe','probability':0.0002},{'name':'Swiss Watch','probability':0.032},{'name':'Neko Helmet','probability':0.045},{'name':'Eternal Rose','probability':0.06},{'name':'Electric Skull','probability':0.08},{'name':'Diamond Ring','probability':0.1},{'name':'Record Player','probability':0.16},{'name':'Love Potion','probability':0.16},{'name':'Top Hat','probability':0.1728},{'name':'Voodoo Doll','probability':0.19}]},{'id':'kissedfrog','name':'Kissed Frog Pond','priceTON':20.0,'imageFilename':'https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/Happy%20Pepe.png','prizes':finalKissedFrogPrizesWithConsolation_Python},{'id':'perfumebottle','name':'Perfume Chest','priceTON':20.0,'prizes':[{'name':'Plush Pepe','probability':0.0004},{'name':'Perfume Bottle','probability':0.02},{'name':'Sharp Tongue','probability':0.035},{'name':'Loot Bag','probability':0.05},{'name':'Swiss Watch','probability':0.06},{'name':'Neko Helmet','probability':0.08},{'name':'Genie Lamp','probability':0.11},{'name':'Kissed Frog','probability':0.15},{'name':'Electric Skull','probability':0.2},{'name':'Diamond Ring','probability':0.2946}]},{'id':'vintagecigar','name':'Vintage Cigar Safe','priceTON':40.0,'prizes':[{'name':'Plush Pepe','probability':0.0008},{'name':'Perfume Bottle','probability':0.025},{'name':'Vintage Cigar','probability':0.03},{'name':'Swiss Watch','probability':0.04},{'name':'Neko Helmet','probability':0.06},{'name':'Sharp Tongue','probability':0.08},{'name':'Genie Lamp','probability':0.1},{'name':'Mini Oscar','probability':0.07},{'name':'Scared Cat','probability':0.2},{'name':'Toy Bear','probability':0.3942}]},{'id':'astralshard','name':'Astral Shard Relic','priceTON':100.0,'prizes':[{'name':'Plush Pepe','probability':0.0015},{'name':'Durov\'s Cap','probability':0.01},{'name':'Astral Shard','probability':0.025},{'name':'Precious Peach','probability':0.025},{'name':'Vintage Cigar','probability':0.04},{'name':'Perfume Bottle','probability':0.05},{'name':'Swiss Watch','probability':0.07},{'name':'Neko Helmet','probability':0.09},{'name':'Mini Oscar','probability':0.06},{'name':'Scared Cat','probability':0.15},{'name':'Loot Bag','probability':0.2},{'name':'Toy Bear','probability':0.2785}]},{'id':'plushpepe','name':'Plush Pepe Hoard','priceTON':200.0,'prizes':[{'name':'Plush Pepe','probability':0.045},{'name':'Durov\'s Cap','probability':0.2},{'name':'Astral Shard','probability':0.755}]}]

for case_data_item in cases_data_backend_with_fixed_prices:
    total_prob = sum(p['probability'] for p in case_data_item['prizes'])
    if total_prob > 0 and abs(total_prob - 1.0) > 0.0001: 
        for prize_item in case_data_item['prizes']:
            prize_item['probability'] = prize_item['probability'] / total_prob

cases_data_backend = []
for case_template in cases_data_backend_with_fixed_prices:
    processed_case = {**case_template}
    if not processed_case.get('isBackgroundCase') and 'imageFilename' not in processed_case :
         processed_case['imageFilename'] = generate_image_filename_from_name(processed_case['name'])
    full_prizes = []
    for prize_stub in processed_case['prizes']:
        prize_name = prize_stub['name']
        image_fn = generate_image_filename_from_name(prize_name)
        if case_template['id'] == 'kissedfrog': # Special handling for Kissed Frog CDN links
            image_fn = f"https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/{prize_name.replace(' ', '%20')}.png"

        full_prizes.append({
            'name': prize_name, 
            'imageFilename': image_fn, 
            'floorPrice': UPDATED_FLOOR_PRICES.get(prize_name, 0), 
            'probability': prize_stub['probability']
        })
    processed_case['prizes'] = full_prizes
    cases_data_backend.append(processed_case)

TON_PRIZE_IMAGE_DEFAULT = "ton-logo.png"
DEFAULT_SLOT_TON_PRIZES = [ {'name': "0.1 TON", 'value': 0.1, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.25 TON", 'value': 0.25, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "0.5 TON", 'value': 0.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.0 TON", 'value': 1.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "1.5 TON", 'value': 1.5, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
PREMIUM_SLOT_TON_PRIZES = [ {'name': "2 TON", 'value': 2.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "3 TON", 'value': 3.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "5 TON", 'value': 5.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, {'name': "10 TON", 'value': 10.0, 'imageFilename': TON_PRIZE_IMAGE_DEFAULT, 'is_ton_prize': True}, ]
ALL_ITEMS_POOL_FOR_SLOTS = [{'name': name, 'floorPrice': price, 'imageFilename': generate_image_filename_from_name(name), 'is_ton_prize': False} for name, price in UPDATED_FLOOR_PRICES.items()]
slots_data_backend = [ { 'id': 'default_slot', 'name': 'Default Slot', 'priceTON': 3.0, 'reels_config': 3, 'prize_pool': [] }, { 'id': 'premium_slot', 'name': 'Premium Slot', 'priceTON': 10.0, 'reels_config': 3, 'prize_pool': [] } ]

def finalize_slot_prize_pools():
    global slots_data_backend
    for slot_data in slots_data_backend:
        temp_pool = []
        if slot_data['id'] == 'default_slot':
            prob_per_ton_prize = (0.50 / len(DEFAULT_SLOT_TON_PRIZES)) if DEFAULT_SLOT_TON_PRIZES else 0
            for ton_prize in DEFAULT_SLOT_TON_PRIZES: temp_pool.append({**ton_prize, 'probability': prob_per_ton_prize})
            item_candidates = [item for item in ALL_ITEMS_POOL_FOR_SLOTS if item['floorPrice'] < 15];
            if not item_candidates: item_candidates = ALL_ITEMS_POOL_FOR_SLOTS[:10]
            remaining_prob_for_items = 0.50
            if item_candidates:
                prob_per_item = remaining_prob_for_items / len(item_candidates)
                for item in item_candidates: temp_pool.append({**item, 'probability': prob_per_item})
        elif slot_data['id'] == 'premium_slot':
            prob_per_ton_prize = (0.40 / len(PREMIUM_SLOT_TON_PRIZES)) if PREMIUM_SLOT_TON_PRIZES else 0
            for ton_prize in PREMIUM_SLOT_TON_PRIZES: temp_pool.append({**ton_prize, 'probability': prob_per_ton_prize})
            item_candidates = [item for item in ALL_ITEMS_POOL_FOR_SLOTS if item['floorPrice'] >= 15];
            if not item_candidates: item_candidates = ALL_ITEMS_POOL_FOR_SLOTS[-10:]
            remaining_prob_for_items = 0.60
            if item_candidates:
                prob_per_item = remaining_prob_for_items / len(item_candidates)
                for item in item_candidates: temp_pool.append({**item, 'probability': prob_per_item})
        current_total_prob = sum(p.get('probability', 0) for p in temp_pool)
        if current_total_prob > 0 and abs(current_total_prob - 1.0) > 0.001:
            for p_item in temp_pool: p_item['probability'] = p_item.get('probability', 0) / current_total_prob
        slot_data['prize_pool'] = temp_pool
finalize_slot_prize_pools()

def calculate_and_log_rtp():
    logger.info("--- RTP Calculations (Based on Current Fixed Prices & Probabilities) ---")
    overall_total_ev_weighted_by_price = Decimal('0'); overall_total_cost_squared = Decimal('0')
    all_games_data = cases_data_backend + slots_data_backend
    for game_data in all_games_data:
        game_id = game_data['id']; game_name = game_data['name']; price = Decimal(str(game_data['priceTON'])); ev = Decimal('0')
        if 'prizes' in game_data:
            for prize in game_data['prizes']:
                prize_value = Decimal(str(UPDATED_FLOOR_PRICES.get(prize['name'], 0)))
                ev += prize_value * Decimal(str(prize['probability']))
        elif 'prize_pool' in game_data:
            for prize_spec in game_data['prize_pool']:
                value = Decimal(str(prize_spec.get('value', prize_spec.get('floorPrice', 0))))
                prob_on_reel = Decimal(str(prize_spec.get('probability', 0)))
                if prize_spec.get('is_ton_prize'): ev += value * prob_on_reel * Decimal(str(game_data.get('reels_config', 3)))
                else: ev += value * (prob_on_reel ** Decimal(str(game_data.get('reels_config', 3))))
        rtp = (ev / price) * 100 if price > 0 else Decimal('0'); dev_cut = 100 - rtp if price > 0 else Decimal('0')
        logger.info(f"Game: {game_name:<25} | Price: {price:>6.2f} TON | Est.EV: {ev:>6.2f} | Est.RTP: {rtp:>6.2f}% | Est.DevCut: {dev_cut:>6.2f}%")
        if price > 0: overall_total_ev_weighted_by_price += ev * price; overall_total_cost_squared += price * price
    if overall_total_cost_squared > 0:
        weighted_avg_rtp = (overall_total_ev_weighted_by_price / overall_total_cost_squared) * 100 if overall_total_cost_squared > 0 else Decimal('0')
        logger.info(f"--- Approx. Weighted Avg RTP (by price, for priced games): {weighted_avg_rtp:.2f}% ---")
    else: logger.info("--- No priced games for overall RTP calculation. ---")

def populate_initial_data():
    db = SessionLocal()
    try:
        for nft_name, floor_price in UPDATED_FLOOR_PRICES.items():
            nft_exists = db.query(NFT).filter(NFT.name == nft_name).first()
            img_filename = generate_image_filename_from_name(nft_name)
            if nft_name in KISSED_FROG_VARIANT_FLOORS: # Use direct CDN link for Kissed Frog variants
                img_filename = f"https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/{nft_name.replace(' ', '%20')}.png"
            if not nft_exists:
                db.add(NFT(name=nft_name, image_filename=img_filename, floor_price=floor_price))
            elif nft_exists.floor_price != floor_price or nft_exists.image_filename != img_filename:
                nft_exists.floor_price = floor_price; nft_exists.image_filename = img_filename
        db.commit()
    except Exception as e:
        db.rollback(); logger.error(f"Error populating initial NFT data: {e}")
    finally: db.close()

def initial_setup_and_logging():
    populate_initial_data()
    db = SessionLocal()
    try:
        if not db.query(PromoCode).filter(PromoCode.code_text == 'Grachev').first():
            db.add(PromoCode(code_text='Grachev', activations_left=10, ton_amount=100.0))
            db.commit()
    except Exception as e:
        db.rollback(); logger.error(f"Error seeding Grachev promocode: {e}")
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

def get_db():
    db = SessionLocal();
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
        data_check_string_parts = [f"{k}={parsed_data[k][0]}" for k in sorted(parsed_data.keys())]
        data_check_string = "\n".join(data_check_string_parts)
        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash_hex == hash_received:
            user_info_dict = json.loads(unquote(parsed_data['user'][0]))
            if 'id' not in user_info_dict: return None
            user_info_dict['id'] = int(user_info_dict['id'])
            return user_info_dict
        else: return None
    except Exception as e_validate:
        logger.error(f"initData validation error: {e_validate}", exc_info=True)
        return None

@app.route('/')
def index_route(): return "Pusik Gifts App is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user:
            user = User(id=uid,username=auth.get("username"),first_name=auth.get("first_name"),last_name=auth.get("last_name"),referral_code=f"ref_{uid}_{random.randint(1000,9999)}")
            db.add(user); db.commit(); db.refresh(user)
        inv = [{"id":i.id,"name":i.nft.name if i.nft else i.item_name_override,"imageFilename":i.nft.image_filename if i.nft else i.item_image_override,"floorPrice":i.nft.floor_price if i.nft else i.current_value,"currentValue":i.current_value,"upgradeMultiplier":i.upgrade_multiplier,"variant":i.variant,"is_ton_prize":i.is_ton_prize,"obtained_at":i.obtained_at.isoformat() if i.obtained_at else None} for i in user.inventory]
        refs = db.query(User).filter(User.referred_by_id == uid).count()
        return jsonify({"id":user.id,"username":user.username,"first_name":user.first_name,"last_name":user.last_name,"tonBalance":user.ton_balance,"starBalance":user.star_balance,"inventory":inv,"referralCode":user.referral_code,"referralEarningsPending":user.referral_earnings_pending,"total_won_ton":user.total_won_ton,"invited_friends_count":refs})
    except Exception as e:
        logger.error(f"Error in get_user_data for {uid}: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); cid = data.get('case_id'); multiplier = int(data.get('multiplier', 1))
    if not cid: return jsonify({"error": "case_id required"}), 400
    if multiplier not in [1,2,3]: return jsonify({"error": "Invalid multiplier"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
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
            if not chosen_prize_info: chosen_prize_info = random.choice(prizes_in_case)
            dbnft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
            if not dbnft: logger.error(f"NFT {chosen_prize_info['name']} missing!"); continue
            variant = None # BLACK Singularity was removed
            actual_val = Decimal(str(dbnft.floor_price))
            total_value_this_spin += actual_val
            item = InventoryItem(user_id=uid,nft_id=dbnft.id,current_value=float(actual_val.quantize(Decimal('0.01'))),variant=variant,is_ton_prize=False)
            db.add(item); db.flush()
            won_prizes_list.append({"id":item.id,"name":dbnft.name,"imageFilename":dbnft.image_filename,"floorPrice":float(dbnft.floor_price),"currentValue":item.current_value,"variant":item.variant,"is_ton_prize":False})
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status":"success","won_prizes":won_prizes_list,"new_balance_ton":user.ton_balance})
    except Exception as e:
        db.rollback(); logger.error(f"Error in open_case: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/spin_slot', methods=['POST'])
def spin_slot_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); slot_id = data.get('slot_id')
    if not slot_id: return jsonify({"error": "slot_id required"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first();
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
            rv = random.random(); cprob = 0; landed_symbol_spec = None
            for p_info_slot in slot_pool:
                cprob += p_info_slot.get('probability', 0)
                if rv <= cprob: landed_symbol_spec = p_info_slot; break
            if not landed_symbol_spec: landed_symbol_spec = random.choice(slot_pool) if slot_pool else None
            if landed_symbol_spec: reel_results_data.append(landed_symbol_spec)
            else: reel_results_data.append({"name":"Error Symbol","imageFilename":"placeholder.png","is_ton_prize":False,"currentValue":0,"floorPrice":0,"value":0})
        won_prizes_from_slot = []; total_value_this_spin = Decimal('0')
        for landed_item_data in reel_results_data:
            if landed_item_data.get('is_ton_prize'):
                ton_val = Decimal(str(landed_item_data['value']))
                temp_ton_prize_id = f"ton_prize_{int(time.time()*1e3)}_{random.randint(0,999)}"
                won_prizes_from_slot.append({"id":temp_ton_prize_id,"name":landed_item_data['name'],"imageFilename":landed_item_data.get('imageFilename',TON_PRIZE_IMAGE_DEFAULT),"currentValue":float(ton_val),"is_ton_prize":True})
                total_value_this_spin += ton_val; user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_val)
        if num_reels == 3 and len(reel_results_data) == 3:
            first_symbol = reel_results_data[0]
            if not first_symbol.get('is_ton_prize') and reel_results_data[1]['name']==first_symbol['name'] and reel_results_data[2]['name']==first_symbol['name']:
                won_item_data_spec = first_symbol
                db_nft = db.query(NFT).filter(NFT.name == won_item_data_spec['name']).first()
                if db_nft:
                    actual_val = Decimal(str(db_nft.floor_price))
                    inv_item = InventoryItem(user_id=uid,nft_id=db_nft.id,current_value=float(actual_val),variant=None,is_ton_prize=False)
                    db.add(inv_item); db.flush()
                    won_prizes_from_slot.append({"id":inv_item.id,"name":db_nft.name,"imageFilename":db_nft.image_filename,"floorPrice":float(db_nft.floor_price),"currentValue":inv_item.current_value,"is_ton_prize":False,"variant":inv_item.variant})
                    total_value_this_spin += actual_val
                else: logger.error(f"Slot win: NFT {won_item_data_spec['name']} not found!")
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status":"success","reel_results":reel_results_data,"won_prizes":won_prizes_from_slot,"new_balance_ton":user.ton_balance})
    except Exception as e:
        db.rollback(); logger.error(f"Error in spin_slot: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id'); mult_str = data.get('multiplier_str')
    if not all([iid, mult_str]): return jsonify({"error": "Missing params"}), 400
    try: mult = Decimal(mult_str); iid_int = int(iid)
    except: return jsonify({"error": "Invalid data"}), 400
    chances = {Decimal("1.5"):50,Decimal("2.0"):35,Decimal("3.0"):25,Decimal("5.0"):15,Decimal("10.0"):8,Decimal("20.0"):3}
    if mult not in chances: return jsonify({"error": "Invalid multiplier"}), 400
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_int, InventoryItem.user_id == uid).first()
        if not item or item.is_ton_prize: return jsonify({"error": "Item not found or not upgradable"}), 404
        user = db.query(User).filter(User.id == uid).first()
        if random.uniform(0,100) < chances[mult]:
            orig_val = Decimal(str(item.current_value)); new_val = (orig_val * mult).quantize(Decimal('0.01'), ROUND_HALF_UP)
            increase = new_val - orig_val; item.current_value = float(new_val)
            item.upgrade_multiplier = float(Decimal(str(item.upgrade_multiplier)) * mult)
            if user: user.total_won_ton = float(Decimal(str(user.total_won_ton)) + increase)
            db.commit()
            return jsonify({"status":"success","message":f"Upgraded! New value: {new_val:.2f} TON","item":{"id":item.id,"currentValue":item.current_value,"name":item.nft.name,"imageFilename":item.nft.image_filename,"upgradeMultiplier":item.upgrade_multiplier,"variant":item.variant }})
        else:
            name_lost = item.nft.name; val_lost = Decimal(str(item.current_value))
            if user: user.total_won_ton = float(Decimal(str(user.total_won_ton)) - val_lost)
            db.delete(item); db.commit()
            return jsonify({"status":"failed","message":f"Upgrade failed! Lost {name_lost}.","item_lost":True})
    except Exception as e:
        db.rollback(); logger.error(f"Error in upgrade_item: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
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
        user = db.query(User).filter(User.id == uid).first()
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_convert_int, InventoryItem.user_id == uid).first()
        if not user or not item: return jsonify({"error": "User or item not found"}), 404
        if item.is_ton_prize: return jsonify({"error": "Cannot convert TON prize."}), 400
        val = Decimal(str(item.current_value)); user.ton_balance = float(Decimal(str(user.ton_balance)) + val)
        item_name_converted = item.nft.name; db.delete(item); db.commit()
        return jsonify({"status":"success","message":f"{item_name_converted} sold for {val:.2f} TON.","new_balance_ton":user.ton_balance})
    except Exception as e:
        db.rollback(); logger.error(f"Error in convert_to_ton: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first()
        if not user: return jsonify({"error": "User not found"}), 404
        items_to_sell = [item_obj for item_obj in user.inventory if not item_obj.is_ton_prize]
        if not items_to_sell: return jsonify({"status":"no_items","message":"No sellable items."})
        total_val = sum(Decimal(str(i_sell.current_value)) for i_sell in items_to_sell)
        user.ton_balance = float(Decimal(str(user.ton_balance)) + total_val)
        for i_del in items_to_sell: db.delete(i_del)
        db.commit()
        return jsonify({"status":"success","message":f"All {len(items_to_sell)} sellable items sold for {total_val:.2f} TON.","new_balance_ton":user.ton_balance})
    except Exception as e:
        db.rollback(); logger.error(f"Error in sell_all_items: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
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
            return jsonify({"error": "You already have an active deposit."}), 409
        nano_part = random.randint(10000, 999999); final_nano_amt = int(orig_amt * 1e9) + nano_part
        pdep = PendingDeposit(user_id=uid,original_amount_ton=orig_amt,unique_identifier_nano_ton=nano_part,final_amount_nano_ton=final_nano_amt,expected_comment=DEPOSIT_COMMENT,expires_at=dt.now(timezone.utc)+timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
        db.add(pdep); db.commit(); db.refresh(pdep)
        disp_amt = f"{final_nano_amt / 1e9:.9f}".rstrip('0').rstrip('.')
        return jsonify({"status":"success","pending_deposit_id":pdep.id,"recipient_address":DEPOSIT_RECIPIENT_ADDRESS_RAW,"amount_to_send":disp_amt,"final_amount_nano_ton":final_nano_amt,"comment":DEPOSIT_COMMENT,"expires_at":pdep.expires_at.isoformat()})
    except Exception as e:
        db.rollback(); logger.error(f"Error in initiate_deposit: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
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
                            if not usr: pdep.status = 'failed'; db_sess.commit(); return {"status":"error","message":"User for deposit not found."}
                            usr.ton_balance = float(Decimal(str(usr.ton_balance)) + Decimal(str(pdep.original_amount_ton)))
                            if usr.referred_by_id:
                                referrer = db_sess.query(User).filter(User.id == usr.referred_by_id).with_for_update().first()
                                if referrer:
                                    referral_bonus = (Decimal(str(pdep.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                    referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + referral_bonus)
                            pdep.status = 'completed'; db_sess.commit()
                            return {"status":"success","message":"Deposit confirmed!","new_balance_ton":usr.ton_balance}
                    except Exception as e: logger.debug(f"Comment parse error for tx {tx.hash}: {e}")
        if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending':
            pdep.status = 'expired'; db_sess.commit()
            return {"status":"expired","message":"Deposit expired."}
        return {"status":"pending","message":"Transaction not confirmed yet."}
    except Exception as e:
        logger.error(f"Blockchain check error: {e}", exc_info=True)
        return {"status":"error","message":"Error checking blockchain."}
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
        if pdep.status == 'expired' or pdep.expires_at <= dt.now(timezone.utc):
            if pdep.status == 'pending': pdep.status = 'expired'; db.commit()
            return jsonify({"status":"expired","message":"Deposit has expired."}), 400
        result = {}
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop)
                result = new_loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
            else: result = loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
        except RuntimeError as e:
            if "cannot be called from a running event loop" in str(e) or "no current event loop" in str(e).lower() or "There is no current event loop in thread" in str(e):
                new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop)
                result = new_loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
            else: logger.error(f"RuntimeError: {e}", exc_info=True); return jsonify({"status":"error","message":"Server error."}), 500
        except Exception as e: logger.error(f"Async exec error: {e}", exc_info=True); return jsonify({"status":"error","message":"Verification error."}), 500
        return jsonify(result)
    except Exception as e:
        db.rollback(); logger.error(f"Outer error in verify_deposit: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db())
    try:
        leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        return jsonify([{"rank":r_idx+1,"name":u_leader.first_name or u_leader.username or f"User_{str(u_leader.id)[:6]}","avatarChar":(u_leader.first_name or u_leader.username or "U")[0].upper(),"income":u_leader.total_won_ton,"user_id":u_leader.id} for r_idx, u_leader in enumerate(leaders)])
    except Exception as e:
        logger.error(f"Error in get_leaderboard: {e}", exc_info=True)
        return jsonify({"error":"Could not load leaderboard"}), 500
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
    except Exception as e:
        db.rollback(); logger.error(f"Error withdrawing ref earnings: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); code_txt = data.get('promocode_text', "").strip()
    if not code_txt: return jsonify({"status":"error","message":"Promocode empty."}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).with_for_update().first()
        if not user: return jsonify({"status":"error","message":"User not found."}), 404
        if not promo: return jsonify({"status":"error","message":"Invalid promocode."}), 404
        if promo.activations_left <= 0: return jsonify({"status":"error","message":"Promocode expired."}), 400
        promo.activations_left -= 1
        user.ton_balance = float(Decimal(str(user.ton_balance)) + Decimal(str(promo.ton_amount)))
        db.commit()
        return jsonify({"status":"success","message":f"Promocode '{code_txt}' redeemed! +{promo.ton_amount:.2f} TON.","new_balance_ton":user.ton_balance})
    except Exception as e:
        db.rollback(); logger.error(f"Error redeeming promocode: {e}", exc_info=True)
        return jsonify({"status":"error","message":"DB error."}), 500
    finally: db.close()

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST'])
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"status":"error","message":"Auth failed"}), 401
    player_user_id = auth_user_data["id"]
    if not TONNEL_SENDER_INIT_DATA: logger.error("Tonnel: TONNEL_SENDER_INIT_DATA not set."); return jsonify({"status":"error","message":"Withdrawal service unavailable."}), 500
    db = next(get_db())
    try:
        item_to_withdraw = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == player_user_id).with_for_update().first()
        if not item_to_withdraw or item_to_withdraw.is_ton_prize: return jsonify({"status":"error","message":"Item not found or not withdrawable."}), 404
        item_name_for_tonnel = item_to_withdraw.nft.name
        tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        tonnel_result = {}
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed(): loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
            tonnel_result = loop.run_until_complete(tonnel_client.send_gift_to_user(gift_item_name=item_name_for_tonnel, receiver_telegram_id=player_user_id))
        except RuntimeError as e:
             if "cannot schedule new futures after shutdown" in str(e).lower() or "event loop is closed" in str(e).lower() :
                new_loop = asyncio.new_event_loop(); asyncio.set_event_loop(new_loop)
                tonnel_result = new_loop.run_until_complete(tonnel_client.send_gift_to_user(gift_item_name=item_name_for_tonnel, receiver_telegram_id=player_user_id))
             else: raise
        if tonnel_result and tonnel_result.get("status") == "success":
            val_deducted = Decimal(str(item_to_withdraw.current_value))
            player = db.query(User).filter(User.id == player_user_id).with_for_update().first()
            if player: player.total_won_ton = float(max(Decimal('0'), Decimal(str(player.total_won_ton)) - val_deducted))
            db.delete(item_to_withdraw); db.commit()
            return jsonify({"status":"success","message":f"Gift '{item_name_for_tonnel}' sent! {tonnel_result.get('message', '')}","details":tonnel_result.get("details")})
        else:
            db.rollback()
            return jsonify({"status":"error","message":f"Tonnel withdrawal failed: {tonnel_result.get('message', 'Tonnel API error')}"}), 500
    except Exception as e:
        db.rollback(); logger.error(f"Tonnel withdrawal wrapper exception: {e}", exc_info=True)
        return jsonify({"status":"error","message":"Unexpected error during withdrawal."}), 500
    finally: db.close()

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
                            try: bot.send_message(referrer.id, f"🎉 Friend {user.first_name or user.username or user.id} joined via your link!")
                            except Exception as e: logger.warning(f"Failed to notify referrer {referrer.id}: {e}")
        except Exception as e: logger.error(f"Error processing start param for {user_id}: {e}")
        updated_fields = False
        if user.username != tg_user_obj.username: user.username = tg_user_obj.username; updated_fields = True
        if user.first_name != tg_user_obj.first_name: user.first_name = tg_user_obj.first_name; updated_fields = True
        if user.last_name != tg_user_obj.last_name: user.last_name = tg_user_obj.last_name; updated_fields = True
        if created_now or updated_fields or (user.referred_by_id and created_now):
            try: db.commit()
            except Exception as e: db.rollback(); logger.error(f"Error committing user {user_id}: {e}")
        button_mini_app_url = f"https://t.me/{bot.get_me().username}/{MINI_APP_NAME or 'app'}"
        markup = types.InlineKeyboardMarkup()
        web_app_info = types.WebAppInfo(url=button_mini_app_url)
        app_button = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app_info)
        markup.add(app_button)
        bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже, чтобы начать!", reply_markup=markup)
    except Exception as e:
        logger.error(f"General error in /start for chat {message.chat.id}: {e}", exc_info=True)
        bot.send_message(message.chat.id, "An error occurred. Try again.")
    finally: db.close()

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.reply_to(message, "Нажмите /start, чтобы открыть Pusik Gifts.")

bot_polling_started = False
bot_polling_thread = None
def run_bot_polling():
    global bot_polling_started
    if bot_polling_started: return
    bot_polling_started = True; logger.info("Starting bot polling...")
    for i in range(3):
        try: bot.remove_webhook(); break
        except Exception as e: logger.warning(f"Attempt {i+1} to remove webhook failed: {e}"); time.sleep(2) if i < 2 else None
    while bot_polling_started:
        try:
            bot.infinity_polling(logger_level=logging.INFO, skip_pending=True, timeout=60, long_polling_timeout=30)
        except telebot.apihelper.ApiTelegramException as e:
            logger.error(f"Telegram API Exception: Code {e.error_code} - {e.description}", exc_info=False)
            if e.error_code == 401 or e.error_code == 409: logger.error("CRITICAL: Bot token invalid/conflict. Stopping."); bot_polling_started = False
            else: time.sleep(30)
        except ConnectionError as e: logger.error(f"Network ConnectionError: {e}", exc_info=False); time.sleep(60)
        except Exception as e: logger.error(f"Unexpected critical error in polling: {type(e).__name__} - {e}", exc_info=True); time.sleep(60)
        if not bot_polling_started: break
        time.sleep(5) 
    logger.info("Bot polling loop terminated.")

if __name__ == '__main__':
    if BOT_TOKEN and not bot_polling_started and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        bot_polling_thread = threading.Thread(target=run_bot_polling, daemon=True)
        bot_polling_thread.start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False, use_reloader=True)
