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

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN") # Still needed for initData validation
DATABASE_URL = os.environ.get("DATABASE_URL")
AUTH_DATE_MAX_AGE_SECONDS = 3600 * 24
TONNEL_SENDER_INIT_DATA = os.environ.get("TONNEL_SENDER_INIT_DATA")
TONNEL_GIFT_SECRET = os.environ.get("TONNEL_GIFT_SECRET", "yowtfisthispieceofshitiiit")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backend_app.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    logger.error("BOT_TOKEN not set for backend (needed for initData validation)!")
    # Not exiting, as some functionalities might not strictly need it if initData validation is bypassed for certain calls
    # but highly recommended to be set.
if not DATABASE_URL:
    logger.error("DATABASE_URL not set!")
    exit("DATABASE_URL is not set. Exiting.")
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
        except json.JSONDecodeError as je_err: # Should be caught by inner try-except for JSON response
            logger.error(f"Tonnel API JSONDecodeError (outer) for {method} {url}: {je_err}") # Should not happen often
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
            purchase_resp=await self._make_request(method="POST",url=buy_gift_url,headers=buy_post_headers,json_payload=buy_payload,timeout=90) # Increased timeout
            if isinstance(purchase_resp,dict)and purchase_resp.get("status")=="success": return {"status":"success","message":f"Gift '{gift_item_name}' sent!","details":purchase_resp}
            else: return {"status":"error","message":f"Tonnel transfer failed: {purchase_resp.get('message','Purchase error') if isinstance(purchase_resp,dict) else 'Unknown error'}"}
        except Exception as e:
            logger.error(f"Tonnel error sending gift '{gift_item_name}' to {receiver_telegram_id}: {type(e).__name__} - {e}", exc_info=True)
            return {"status":"error","message":f"Unexpected error during Tonnel withdrawal: {str(e)}"}
        finally: await self._close_session_if_open()

TON_PRIZE_IMAGE_DEFAULT = "https://case-bot.com/images/actions/ton.svg"
GIFT_NAME_TO_ID_MAP_PY = { "Santa Hat": "5983471780763796287", "Signet Ring": "5936085638515261992", "Precious Peach": "5933671725160989227", "Plush Pepe": "5936013938331222567", "Spiced Wine": "5913442287462908725", "Jelly Bunny": "5915502858152706668", "Durov's Cap": "5915521180483191380", "Perfume Bottle": "5913517067138499193", "Eternal Rose": "5882125812596999035", "Berry Box": "5882252952218894938", "Vintage Cigar": "5857140566201991735", "Magic Potion": "5846226946928673709", "Kissed Frog": "5845776576658015084", "Hex Pot": "5825801628657124140", "Evil Eye": "5825480571261813595", "Sharp Tongue": "5841689550203650524", "Trapped Heart": "5841391256135008713", "Skull Flower": "5839038009193792264", "Scared Cat": "5837059369300132790", "Spy Agaric": "5821261908354794038", "Homemade Cake": "5783075783622787539", "Genie Lamp": "5933531623327795414", "Lunar Snake": "6028426950047957932", "Party Sparkler": "6003643167683903930", "Jester Hat": "5933590374185435592", "Witch Hat": "5821384757304362229", "Hanging Star": "5915733223018594841", "Love Candle": "5915550639663874519", "Cookie Heart": "6001538689543439169", "Desk Calendar": "5782988952268964995", "Jingle Bells": "6001473264306619020", "Snow Mittens": "5980789805615678057", "Voodoo Doll": "5836780359634649414", "Mad Pumpkin": "5841632504448025405", "Hypno Lollipop": "5825895989088617224", "B-Day Candle": "5782984811920491178", "Bunny Muffin": "5935936766358847989", "Astral Shard": "5933629604416717361", "Flying Broom": "5837063436634161765", "Crystal Ball": "5841336413697606412", "Eternal Candle": "5821205665758053411", "Swiss Watch": "5936043693864651359", "Ginger Cookie": "5983484377902875708", "Mini Oscar": "5879737836550226478", "Lol Pop": "5170594532177215681", "Ion Gem": "5843762284240831056", "Star Notepad": "5936017773737018241", "Loot Bag": "5868659926187901653", "Love Potion": "5868348541058942091", "Toy Bear": "5868220813026526561", "Diamond Ring": "5868503709637411929", "Sakura Flower": "5167939598143193218", "Sleigh Bell": "5981026247860290310", "Top Hat": "5897593557492957738", "Record Player": "5856973938650776169", "Winter Wreath": "5983259145522906006", "Snow Globe": "5981132629905245483", "Electric Skull": "5846192273657692751", "Tama Gadget": "6023752243218481939", "Candy Cane": "6003373314888696650", "Neko Helmet": "5933793770951673155", "Jack-in-the-Box": "6005659564635063386", "Easter Egg": "5773668482394620318", "Bonded Ring": "5870661333703197240", "Pet Snake": "6023917088358269866", "Snake Box": "6023679164349940429", "Xmas Stocking": "6003767644426076664", "Big Year": "6028283532500009446", "Holiday Drink": "6003735372041814769", "Gem Signet": "5859442703032386168", "Light Sword": "5897581235231785485"}
GIFT_NAME_TO_ID_MAP_PY["Durov's Cap"] = "5915521180483191380" # Ensure specific cases are handled

def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png'
    if "TON" in name_str.upper() and ("PRIZE" in name_str.upper() or name_str.replace('.', '', 1).replace(' TON', '').strip().isdigit()):
        return TON_PRIZE_IMAGE_DEFAULT
    gift_id = GIFT_NAME_TO_ID_MAP_PY.get(name_str)
    if gift_id:
        return f"https://cdn.changes.tg/gifts/originals/{gift_id}/Original.png"
    if name_str in KISSED_FROG_VARIANT_FLOORS: # Check after direct map for generic Kissed Frog
        return f"https://cdn.changes.tg/gifts/models/Kissed%20Frog/png/{name_str.replace(' ', '%20')}.png"
    
    # Handle specific name formatting if not found by ID
    if name_str == "Durov's Cap": return "Durov's-Cap.png" # This will likely not be hit if ID map works
    if name_str == "Vintage Cigar": return "Vintage-Cigar.png" # Same as above
    name_str_rep = name_str.replace('-', '_')
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png'

    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""))
    filename = re.sub(r'-+', '-', cleaned) # Consolidate multiple hyphens
    if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg')):
        filename += '.png'
    return filename # This will be a relative path for frontend; backend doesn't serve these directly

UPDATED_FLOOR_PRICES = {'Plush Pepe':1200.0,'Neko Helmet':15.0,'Sharp Tongue':17.0,"Durov's Cap":251.0,'Voodoo Doll':9.4,'Vintage Cigar':19.7,'Astral Shard':50.0,'Scared Cat':22.0,'Swiss Watch':18.6,'Perfume Bottle':38.3,'Precious Peach':100.0,'Toy Bear':16.3,'Genie Lamp':19.3,'Loot Bag':25.0,'Kissed Frog':14.8,'Electric Skull':10.9,'Diamond Ring':8.06,'Mini Oscar':40.5,'Party Sparkler':2.0,'Homemade Cake':2.0,'Cookie Heart':1.8,'Jack-in-the-box':2.0,'Skull Flower':3.4,'Lol Pop':1.4,'Hynpo Lollipop':1.4,'Desk Calendar':1.4,'B-Day Candle':1.4,'Record Player':4.0,'Jelly Bunny':3.6,'Tama Gadget':4.0,'Snow Globe':4.0,'Eternal Rose':11.0,'Love Potion':5.4,'Top Hat':6.0}
KISSED_FROG_VARIANT_FLOORS = {"Happy Pepe":500.0,"Tree Frog":150.0,"Brewtoad":150.0,"Puddles":150.0,"Honeyhop":150.0,"Melty Butter":150.0,"Lucifrog":150.0,"Zodiak Croak":150.0,"Count Croakula":150.0,"Lilie Pond":150.0,"Sweet Dream":150.0,"Frogmaid":150.0,"Rocky Hopper":150.0,"Icefrog":45.0,"Lava Leap":45.0,"Toadstool":45.0,"Desert Frog":45.0,"Cupid":45.0,"Hopberry":45.0,"Ms. Toad":45.0,"Trixie":45.0,"Prince Ribbit":45.0,"Pond Fairy":45.0,"Boingo":45.0,"Tesla Frog":45.0,"Starry Night":30.0,"Silver":30.0,"Ectofrog":30.0,"Poison":30.0,"Minty Bloom":30.0,"Sarutoad":30.0,"Void Hopper":30.0,"Ramune":30.0,"Lemon Drop":30.0,"Ectobloom":30.0,"Duskhopper":30.0,"Bronze":30.0,"Lily Pond":19.0,"Toadberry":19.0,"Frogwave":19.0,"Melon":19.0,"Sky Leaper":19.0,"Frogtart":19.0,"Peach":19.0,"Sea Breeze":19.0,"Lemon Juice":19.0,"Cranberry":19.0,"Tide Pod":19.0,"Brownie":19.0,"Banana Pox":19.0}
UPDATED_FLOOR_PRICES.update(KISSED_FROG_VARIANT_FLOORS)

finalKissedFrogPrizesWithConsolation_Python=[
    {'name':'Happy Pepe','probability':0.0000676},
    {'name':'Tree Frog','probability':0.0003378},
    {'name':'Brewtoad','probability':0.0003378},
    {'name':'Puddles','probability':0.0003378},
    {'name':'Honeyhop','probability':0.0003378},
    {'name':'Melty Butter','probability':0.0003378},
    {'name':'Lucifrog','probability':0.0003378},
    {'name':'Zodiak Croak','probability':0.0003378},
    {'name':'Count Croakula','probability':0.0003378},
    {'name':'Lilie Pond','probability':0.0003378},
    {'name':'Sweet Dream','probability':0.0003378},
    {'name':'Frogmaid','probability':0.0003378},
    {'name':'Rocky Hopper','probability':0.0003378},
    {'name':'Icefrog','probability':0.0013511},
    {'name':'Lava Leap','probability':0.0013511},
    {'name':'Toadstool','probability':0.0013511},
    {'name':'Desert Frog','probability':0.0013511},
    {'name':'Cupid','probability':0.0013511},
    {'name':'Hopberry','probability':0.0013511},
    {'name':'Ms. Toad','probability':0.0013511},
    {'name':'Trixie','probability':0.0013511},
    {'name':'Prince Ribbit','probability':0.0013511},
    {'name':'Pond Fairy','probability':0.0013511},
    {'name':'Boingo','probability':0.0013511},
    {'name':'Tesla Frog','probability':0.0013511},
    {'name':'Starry Night','probability':0.0047291},
    {'name':'Silver','probability':0.0047291},
    {'name':'Ectofrog','probability':0.0047291},
    {'name':'Poison','probability':0.0047291},
    {'name':'Minty Bloom','probability':0.0047291},
    {'name':'Sarutoad','probability':0.0047291},
    {'name':'Void Hopper','probability':0.0047291},
    {'name':'Ramune','probability':0.0047291},
    {'name':'Lemon Drop','probability':0.0047291},
    {'name':'Ectobloom','probability':0.0047291},
    {'name':'Duskhopper','probability':0.0047291},
    {'name':'Bronze','probability':0.0047291},
    {'name':'Lily Pond','probability':0.0271578},
    {'name':'Toadberry','probability':0.0271578},
    {'name':'Frogwave','probability':0.0271578},
    {'name':'Melon','probability':0.0271578},
    {'name':'Sky Leaper','probability':0.0271578},
    {'name':'Frogtart','probability':0.0271578},
    {'name':'Peach','probability':0.0271578},
    {'name':'Sea Breeze','probability':0.0271578},
    {'name':'Lemon Juice','probability':0.0271578},
    {'name':'Cranberry','probability':0.0271578},
    {'name':'Tide Pod','probability':0.0271578},
    {'name':'Brownie','probability':0.0271578},
    {'name':'Banana Pox','probability':0.0271548},
    {'name':'Desk Calendar','probability':0.0000000}
]
currentProbSumForKF_py=sum(p['probability']for p in finalKissedFrogPrizesWithConsolation_Python)
remainingProbForConsolation_py=1.0-currentProbSumForKF_py
if remainingProbForConsolation_py>0.00001:finalKissedFrogPrizesWithConsolation_Python.append({'name':"Desk Calendar",'probability':remainingProbForConsolation_py})

cases_data_backend_with_fixed_prices=[
    {'id':'lolpop','name':'Lol Pop Stash','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Lol-Pop.jpg','priceTON':2.0,'prizes':[{'name':'Plush Pepe','probability':0.0000405},{'name':'Neko Helmet','probability':0.0012134},{'name':'Party Sparkler','probability':0.0931264},{'name':'Homemade Cake','probability':0.0931264},{'name':'Cookie Heart','probability':0.0931264},{'name':'Jack-in-the-box','probability':0.0647313},{'name':'Skull Flower','probability':0.0283281},{'name':'Lol Pop','probability':0.1782297},{'name':'Hynpo Lollipop','probability':0.1765275},{'name':'Desk Calendar','probability':0.0404499},{'name':'B-Day Candle','probability':0.0404499}]},
    {'id':'recordplayer','name':'Record Player Vault','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Record-Player.jpg','priceTON':6.0,'prizes':[{'name':'Plush Pepe','probability':0.0002123},{'name':'Record Player','probability':0.3400588},{'name':'Lol Pop','probability':0.2123},{'name':'Hynpo Lollipop','probability':0.2123},{'name':'Party Sparkler','probability':0.1837833},{'name':'Skull Flower','probability':0.1415333},{'name':'Jelly Bunny','probability':0.1413346},{'name':'Tama Gadget','probability':0.0990733},{'name':'Snow Globe','probability':0.0850333}]},
    {'id':'swisswatch','name':'Swiss Watch Box','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Swiss-Watch.jpg','priceTON':10.0,'prizes':[{'name':'Plush Pepe','probability':0.0001768},{'name':'Swiss Watch','probability':0.0282688},{'name':'Neko Helmet','probability':0.0396556},{'name':'Eternal Rose','probability':0.0528741},{'name':'Electric Skull','probability':0.0704988},{'name':'Diamond Ring','probability':0.0881235},{'name':'Record Player','probability':0.1410},{'name':'Love Potion','probability':0.1410},{'name':'Top Hat','probability':0.151978},{'name':'Voodoo Doll','probability':0.1668841}]}, # FIXED: 'probability' is now quoted here
    {'id':'kissedfrog','name':'Kissed Frog Pond','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Kissed-Frog.jpg','priceTON':20.0,'prizes':finalKissedFrogPrizesWithConsolation_Python}, # This now refers to the fixed list above
    {'id':'perfumebottle','name':'Perfume Chest','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Perfume-Bottle.jpg','priceTON':20.0,'prizes':[{'name':'Plush Pepe','probability':0.000402},{'name':'Perfume Bottle','probability':0.0201000},{'name':'Sharp Tongue','probability':0.0351750},{'name':'Loot Bag','probability':0.05025},{'name':'Swiss Watch','probability':0.0603000},{'name':'Neko Helmet','probability':0.0804000},{'name':'Genie Lamp','probability':0.1105500},{'name':'Kissed Frog','probability':0.1507500},{'name':'Electric Skull','probability':0.2010000},{'name':'Diamond Ring','probability':0.2970599}]},
    {'id':'vintagecigar','name':'Vintage Cigar Safe','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Vintage-Cigar.jpg','priceTON':40.0,'prizes':[{'name':'Plush Pepe','probability':0.0010901},{'name':'Perfume Bottle','probability':0.0340656},{'name':'Vintage Cigar','probability':0.0408788},{'name':'Swiss Watch','probability':0.0545051},{'name':'Neko Helmet','probability':0.0817577},{'name':'Sharp Tongue','probability':0.1090103},{'name':'Genie Lamp','probability':0.1362629},{'name':'Mini Oscar','probability':0.095384},{'name':'Scared Cat','probability':0.2725257},{'name':'Toy Bear','probability':0.5369798}]},
    {'id':'astralshard','name':'Astral Shard Relic','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Astral-Shard.jpg','priceTON':100.0,'prizes':[{'name':'Plush Pepe','probability':0.0044569},{'name':'Durov\'s Cap','probability':0.02946},{'name':'Astral Shard','probability':0.0736500},{'name':'Precious Peach','probability':0.0736500},{'name':'Vintage Cigar','probability':0.1178400},{'name':'Perfume Bottle','probability':0.1473000},{'name':'Swiss Watch','probability':0.2062200},{'name':'Neko Helmet','probability':0.2647400},{'name':'Mini Oscar','probability':0.1779600},{'name':'Scared Cat','probability':0.4456500},{'name':'Loot Bag','probability':0.5942000},{'name':'Toy Bear','probability':0.8260331}]},
    {'id':'plushpepe','name':'Plush Pepe Hoard','imageFilename':'https://raw.githubusercontent.com/Vasiliy-katsyka/case/main/caseImages/Plush-Pepe.jpg','priceTON':200.0,'prizes':[{'name':'Plush Pepe','probability':0.0454395},{'name':'Durov\'s Cap','probability':0.2019445},{'name':'Astral Shard','probability':0.9634789}]}
]

for case_data_item in cases_data_backend_with_fixed_prices:
    total_prob = sum(p['probability'] for p in case_data_item['prizes'])
    if total_prob > 0 and abs(total_prob - 1.0) > 0.0001: 
        for prize_item in case_data_item['prizes']:
            prize_item['probability'] = prize_item['probability'] / total_prob

cases_data_backend = []
for case_template in cases_data_backend_with_fixed_prices:
    processed_case = {**case_template} # Shallow copy
    # If imageFilename is a direct URL, keep it. Otherwise, generate based on name.
    # The frontend uses a different base URL for relative paths. Backend doesn't serve these images.
    if 'imageFilename' not in processed_case or not processed_case['imageFilename'].startswith('http'):
         # This path is mostly for local reference or if frontend logic was to build from it.
        processed_case['imageFilename'] = generate_image_filename_from_name(processed_case.get('imageFilename', processed_case['name']))

    full_prizes = []
    for prize_stub in processed_case['prizes']:
        prize_name = prize_stub['name']
        # image_fn_prize will be determined by generate_image_filename_from_name logic
        # which already handles CDN links for known gifts.
        image_fn_prize = generate_image_filename_from_name(prize_name)
        
        full_prizes.append({
            'name': prize_name, 
            'imageFilename': image_fn_prize, # This will be the CDN URL or a generated name
            'floorPrice': UPDATED_FLOOR_PRICES.get(prize_name, 0), 
            'probability': prize_stub['probability']
        })
    processed_case['prizes'] = full_prizes
    cases_data_backend.append(processed_case)

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
            if not item_candidates: item_candidates = ALL_ITEMS_POOL_FOR_SLOTS[:10] # Fallback
            remaining_prob_for_items = 0.50
            if item_candidates:
                prob_per_item = remaining_prob_for_items / len(item_candidates)
                for item in item_candidates: temp_pool.append({**item, 'probability': prob_per_item})
        elif slot_data['id'] == 'premium_slot':
            prob_per_ton_prize = (0.40 / len(PREMIUM_SLOT_TON_PRIZES)) if PREMIUM_SLOT_TON_PRIZES else 0
            for ton_prize in PREMIUM_SLOT_TON_PRIZES: temp_pool.append({**ton_prize, 'probability': prob_per_ton_prize})
            item_candidates = [item for item in ALL_ITEMS_POOL_FOR_SLOTS if item['floorPrice'] >= 15];
            if not item_candidates: item_candidates = ALL_ITEMS_POOL_FOR_SLOTS[-10:] # Fallback
            remaining_prob_for_items = 0.60
            if item_candidates:
                prob_per_item = remaining_prob_for_items / len(item_candidates)
                for item in item_candidates: temp_pool.append({**item, 'probability': prob_per_item})
        
        current_total_prob = sum(p.get('probability', 0) for p in temp_pool)
        if current_total_prob > 0 and abs(current_total_prob - 1.0) > 0.001: # Normalize if not exactly 1.0
            for p_item in temp_pool: p_item['probability'] = p_item.get('probability', 0) / current_total_prob
        slot_data['prize_pool'] = temp_pool
finalize_slot_prize_pools()


def calculate_and_log_rtp():
    logger.info("--- RTP Calculations (Based on Current Fixed Prices & Probabilities) ---")
    overall_total_ev_weighted_by_price = Decimal('0'); overall_total_cost_squared = Decimal('0')
    all_games_data = cases_data_backend + slots_data_backend
    for game_data in all_games_data:
        game_id = game_data['id']; game_name = game_data['name']; price = Decimal(str(game_data['priceTON'])); ev = Decimal('0')
        if 'prizes' in game_data: # This is a Case
            for prize in game_data['prizes']:
                prize_value = Decimal(str(UPDATED_FLOOR_PRICES.get(prize['name'], 0)))
                ev += prize_value * Decimal(str(prize['probability']))
        elif 'prize_pool' in game_data: # This is a Slot
            # Simplified EV for slots: sum(value * prob_on_reel * num_reels_if_ton OR value * prob_on_reel^num_reels_if_nft_match)
            # This is a complex calculation if aiming for true RTP for slots matching game.
            # For now, let's do a simpler weighted average of prize values in the pool for display.
            # True slot RTP requires considering probabilities of symbol combinations.
            # The current EV calculation for slots in the original code was:
            # ev += value * prob_on_reel * Decimal(str(game_data.get('reels_config', 3))) for TON
            # ev += value * (prob_on_reel ** Decimal(str(game_data.get('reels_config', 3)))) for NFT
            # This assumes TON prizes are additive per reel showing them, and NFTs require all reels to match.
            for prize_spec in game_data['prize_pool']:
                value = Decimal(str(prize_spec.get('value', prize_spec.get('floorPrice', 0))))
                prob_on_reel = Decimal(str(prize_spec.get('probability', 0))) # Probability of this symbol appearing on ONE reel
                num_reels_for_slot = Decimal(str(game_data.get('reels_config', 3)))

                if prize_spec.get('is_ton_prize'): 
                    # Assuming if a TON symbol appears on any reel, you get its value (simplistic interpretation for this RTP log)
                    # A more accurate interpretation might be 1 - (1-prob_on_reel)^num_reels_for_slot for "at least one TON symbol"
                    # or (prob_on_reel * num_reels_for_slot) if each reel pays independently (unlikely for a single prize name)
                    # Let's stick to the original EV logic for consistency in this log
                    ev_contribution_ton = value * prob_on_reel * num_reels_for_slot # This might overestimate if not independent per reel
                    ev += ev_contribution_ton
                else: 
                    # For NFTs, assume all reels must match
                    prob_all_match = prob_on_reel ** num_reels_for_slot
                    ev_contribution_nft = value * prob_all_match
                    ev += ev_contribution_nft
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
            img_filename_or_url = generate_image_filename_from_name(nft_name) # Will give CDN or generated name
            
            if not nft_exists:
                db.add(NFT(name=nft_name, image_filename=img_filename_or_url, floor_price=floor_price))
            elif nft_exists.floor_price != floor_price or nft_exists.image_filename != img_filename_or_url:
                nft_exists.floor_price = floor_price
                nft_exists.image_filename = img_filename_or_url # Update if different
        db.commit()
    except Exception as e:
        db.rollback(); logger.error(f"Error populating initial NFT data: {e}")
    finally: db.close()

def initial_setup_and_logging():
    populate_initial_data()
    db = SessionLocal()
    try:
        # Seed a default promo code if it doesn't exist
        if not db.query(PromoCode).filter(PromoCode.code_text == 'Grachev').first():
            db.add(PromoCode(code_text='Grachev', activations_left=10, ton_amount=100.0)) # Example
            db.commit()
            logger.info("Seeded 'Grachev' promocode.")
    except Exception as e:
        db.rollback(); logger.error(f"Error seeding Grachev promocode: {e}")
    finally: db.close()
    calculate_and_log_rtp()

initial_setup_and_logging() # Run once at startup

DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR" # Your fixed deposit address
DEPOSIT_COMMENT = "cpd7r07ud3s" # Your fixed deposit comment
PENDING_DEPOSIT_EXPIRY_MINUTES = 30 # How long a deposit request is valid

app = Flask(__name__)
PROD_ORIGIN = "https://vasiliy-katsyka.github.io"; NULL_ORIGIN = "null" 
LOCAL_DEV_ORIGINS = ["http://localhost:5500","http://127.0.0.1:5500","http://localhost:8000","http://127.0.0.1:8000",]
final_allowed_origins = list(set([PROD_ORIGIN, NULL_ORIGIN] + LOCAL_DEV_ORIGINS))
CORS(app, resources={r"/api/*": {"origins": final_allowed_origins}})


def get_db():
    db = SessionLocal();
    try: yield db
    finally: db.close()

def validate_init_data(init_data_str: str, bot_token_for_validation: str) -> dict | None:
    logger.debug(f"Attempting to validate initData: {init_data_str[:200]}...")
    try:
        if not init_data_str:
            logger.warning("validate_init_data: init_data_str is empty or None.")
            return None

        parsed_data = dict(parse_qs(init_data_str))
        
        required_keys = ['hash', 'user', 'auth_date']
        missing_keys = [k for k in required_keys if k not in parsed_data]
        if missing_keys:
            logger.warning(f"validate_init_data: Missing keys in parsed_data: {missing_keys}. Parsed: {list(parsed_data.keys())}")
            return None

        hash_received = parsed_data.pop('hash')[0]
        auth_date_ts = int(parsed_data['auth_date'][0])
        current_ts = int(dt.now(timezone.utc).timestamp())

        if (current_ts - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS:
            logger.warning(f"validate_init_data: auth_date expired. auth_date_ts: {auth_date_ts}, current_ts: {current_ts}, diff: {current_ts - auth_date_ts}s, max_age: {AUTH_DATE_MAX_AGE_SECONDS}s")
            return None

        data_check_string_parts = [f"{k}={parsed_data[k][0]}" for k in sorted(parsed_data.keys())]
        data_check_string = "\n".join(data_check_string_parts)

        secret_key = hmac.new("WebAppData".encode(), bot_token_for_validation.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if calculated_hash_hex == hash_received:
            user_info_str_unquoted = unquote(parsed_data['user'][0])
            try:
                user_info_dict = json.loads(user_info_str_unquoted)
            except json.JSONDecodeError as je:
                logger.error(f"validate_init_data: Failed to parse user JSON: {user_info_str_unquoted}. Error: {je}")
                return None
            
            if 'id' not in user_info_dict:
                logger.warning(f"validate_init_data: 'id' not found in user_info_dict. User data: {user_info_dict}")
                return None
            
            user_info_dict['id'] = int(user_info_dict['id']) # Ensure id is int
            logger.info(f"validate_init_data: Hash matched for user ID: {user_info_dict.get('id')}. Auth successful.")
            return user_info_dict
        else:
            logger.warning(f"validate_init_data: Hash mismatch.")
            logger.debug(f"Received Hash: {hash_received}")
            logger.debug(f"Calculated Hash: {calculated_hash_hex}")
            logger.debug(f"Data Check String: {data_check_string[:500]}")
            logger.debug(f"BOT_TOKEN used for secret_key (first 5 chars): {bot_token_for_validation[:5]}...")
            return None
    except Exception as e_validate:
        logger.error(f"validate_init_data: General exception: {e_validate}", exc_info=True)
        return None

@app.route('/')
def index_route(): return "Pusik Gifts API Backend is Running!"

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
        
        # Update user info if changed from Telegram
        changed = False
        if user.username != auth.get("username"): user.username = auth.get("username"); changed=True
        if user.first_name != auth.get("first_name"): user.first_name = auth.get("first_name"); changed=True
        if user.last_name != auth.get("last_name"): user.last_name = auth.get("last_name"); changed=True
        if changed: db.commit(); db.refresh(user)

        inv = [{"id":i.id,"name":i.nft.name if i.nft else i.item_name_override,"imageFilename":i.nft.image_filename if i.nft else i.item_image_override,"floorPrice":i.nft.floor_price if i.nft else i.current_value,"currentValue":i.current_value,"upgradeMultiplier":i.upgrade_multiplier,"variant":i.variant,"is_ton_prize":i.is_ton_prize,"obtained_at":i.obtained_at.isoformat() if i.obtained_at else None} for i in user.inventory]
        refs_count = db.query(User).filter(User.referred_by_id == uid).count()
        return jsonify({"id":user.id,"username":user.username,"first_name":user.first_name,"last_name":user.last_name,"tonBalance":user.ton_balance,"starBalance":user.star_balance,"inventory":inv,"referralCode":user.referral_code,"referralEarningsPending":user.referral_earnings_pending,"total_won_ton":user.total_won_ton,"invited_friends_count":refs_count})
    except Exception as e:
        logger.error(f"Error in get_user_data for {uid}: {e}", exc_info=True)
        return jsonify({"error": "DB error"}), 500
    finally: db.close()

@app.route('/api/register_referral', methods=['POST'])
def register_referral_api():
    data = flask_request.get_json()
    user_id = data.get('user_id')
    username = data.get('username')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    referral_code_used = data.get('referral_code')

    if not all([user_id, referral_code_used]):
        return jsonify({"error": "Missing user_id or referral_code"}), 400
    
    db = next(get_db())
    try:
        referred_user = db.query(User).filter(User.id == user_id).first()
        if not referred_user:
            referred_user = User(
                id=user_id, 
                username=username, 
                first_name=first_name, 
                last_name=last_name,
                referral_code=f"ref_{user_id}_{random.randint(1000,9999)}" # Generate own code
            )
            db.add(referred_user)
            # db.flush() # Get ID if autogen, but here ID is from TG
        else: # User exists, update names if changed
            if referred_user.username != username: referred_user.username = username
            if referred_user.first_name != first_name: referred_user.first_name = first_name
            if referred_user.last_name != last_name: referred_user.last_name = last_name
        
        if referred_user.referred_by_id: # Already referred
            db.commit() # Commit potential name updates
            return jsonify({"status": "already_referred", "message": "User was already referred."}), 200

        referrer = db.query(User).filter(User.referral_code == referral_code_used).first()
        if not referrer:
            db.commit() # Commit new user or name updates
            return jsonify({"error": "Referrer not found with this code."}), 404
        
        if referrer.id == referred_user.id: # Cannot refer self
            db.commit() # Commit new user or name updates
            return jsonify({"error": "Cannot refer oneself."}), 400

        referred_user.referred_by_id = referrer.id
        db.commit()
        logger.info(f"User {user_id} successfully referred by {referrer.id} using code {referral_code_used}")
        
        # Optionally, trigger a notification to the referrer here if desired (e.g. via a message queue or direct bot call if bot has an API)
        # For simplicity, this example doesn't include direct bot notification from backend.
        
        return jsonify({"status": "success", "message": "Referral registered successfully."}), 200
    except IntegrityError as ie:
        db.rollback()
        logger.error(f"Integrity error registering referral for {user_id} with code {referral_code_used}: {ie}", exc_info=True)
        return jsonify({"error": "Database integrity error, possibly concurrent registration."}), 409
    except Exception as e:
        db.rollback()
        logger.error(f"Error registering referral for {user_id} with code {referral_code_used}: {e}", exc_info=True)
        return jsonify({"error": "Server error during referral registration."}), 500
    finally:
        db.close()

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); cid = data.get('case_id'); multiplier = int(data.get('multiplier', 1))
    if not cid: return jsonify({"error": "case_id required"}), 400
    if multiplier not in [1,2,3]: return jsonify({"error": "Invalid multiplier"}), 400
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
        for _ in range(multiplier):
            rv = random.random(); cprob = 0; chosen_prize_info = None
            for p_info in prizes_in_case:
                cprob += p_info['probability']
                if rv <= cprob: chosen_prize_info = p_info; break
            if not chosen_prize_info: chosen_prize_info = random.choice(prizes_in_case) # Fallback
            
            dbnft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
            if not dbnft: 
                logger.error(f"NFT {chosen_prize_info['name']} missing from DB during case open for case {cid}!"); 
                # Give a default consolation prize or skip
                # For now, skipping this specific prize win if NFT definition is missing
                continue 
            
            variant = None # Logic for variants if Kissed Frog or similar
            if dbnft.name == "Kissed Frog": # This means a specific variant was chosen from the 'kissedfrog' case pool
                variant = chosen_prize_info['name'] # The name itself is the variant
                # Ensure the base "Kissed Frog" NFT entry is used for nft_id if variants are stored differently
                # Assuming chosen_prize_info['name'] is a valid NFT.name in the DB
            
            actual_val = Decimal(str(dbnft.floor_price)) # Use floor price from DB
            total_value_this_spin += actual_val
            
            item = InventoryItem(
                user_id=uid,
                nft_id=dbnft.id,
                current_value=float(actual_val.quantize(Decimal('0.01'))),
                variant=variant, # Store the variant name if applicable
                is_ton_prize=False,
                item_name_override=chosen_prize_info['name'] if variant else None # Store full variant name if different from base NFT
            )
            db.add(item); db.flush() # Get item.id
            
            won_prizes_list.append({
                "id":item.id,
                "name":chosen_prize_info['name'], # Full name including variant
                "imageFilename":chosen_prize_info['imageFilename'], # Image for the specific variant/prize
                "floorPrice":float(dbnft.floor_price), # Base floor price or specific variant floor
                "currentValue":item.current_value,
                "variant":item.variant,
                "is_ton_prize":False
            })
        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status":"success","won_prizes":won_prizes_list,"new_balance_ton":user.ton_balance})
    except Exception as e:
        db.rollback(); logger.error(f"Error in open_case: {e}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
    finally: db.close()

@app.route('/api/spin_slot', methods=['POST'])
def spin_slot_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); slot_id = data.get('slot_id')
    if not slot_id: return jsonify({"error": "slot_id required"}), 400
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first();
        if not user: return jsonify({"error": "User not found"}), 404
        target_slot = next((s for s in slots_data_backend if s['id'] == slot_id), None)
        if not target_slot: return jsonify({"error": "Slot not found"}), 404
        cost = Decimal(str(target_slot['priceTON']))
        if Decimal(str(user.ton_balance)) < cost: return jsonify({"error": f"Not enough TON. Need {cost:.2f}"}), 400
        user.ton_balance = float(Decimal(str(user.ton_balance)) - cost)
        num_reels = target_slot.get('reels_config', 3); slot_pool = target_slot['prize_pool']
        if not slot_pool: return jsonify({"error": "Slot prize pool empty"}), 500
        
        reel_results_data = [] # This will store the spec of the symbol that landed on each reel
        for _ in range(num_reels):
            rv = random.random(); cprob = 0; landed_symbol_spec = None
            for p_info_slot in slot_pool:
                cprob += p_info_slot.get('probability', 0)
                if rv <= cprob: landed_symbol_spec = p_info_slot; break
            if not landed_symbol_spec: landed_symbol_spec = random.choice(slot_pool) if slot_pool else None # Fallback
            
            if landed_symbol_spec: reel_results_data.append(landed_symbol_spec)
            else: reel_results_data.append({"name":"Error Symbol","imageFilename":"placeholder.png","is_ton_prize":False,"currentValue":0,"floorPrice":0,"value":0}) # Should not happen
            
        won_prizes_from_slot = []; total_value_this_spin = Decimal('0')
        
        # Check for TON prizes on any reel (simplified: if a TON symbol lands, you get its value)
        # This interpretation assumes each TON symbol appearing pays out.
        # More complex slots might require specific combinations of TON symbols.
        for landed_item_data in reel_results_data:
            if landed_item_data.get('is_ton_prize'):
                ton_val = Decimal(str(landed_item_data['value']))
                # TON prizes are directly added to balance, not inventory typically
                temp_ton_prize_id = f"ton_prize_{int(time.time()*1e3)}_{random.randint(0,999)}" # For display in win overlay
                won_prizes_from_slot.append({
                    "id": temp_ton_prize_id, # Transient ID for display
                    "name": landed_item_data['name'],
                    "imageFilename": landed_item_data.get('imageFilename', TON_PRIZE_IMAGE_DEFAULT),
                    "currentValue": float(ton_val),
                    "is_ton_prize": True
                })
                total_value_this_spin += ton_val
                user.ton_balance = float(Decimal(str(user.ton_balance)) + ton_val) # Add directly to balance
        
        # Check for NFT prize (all reels matching a non-TON symbol)
        if num_reels == 3 and len(reel_results_data) == 3: # Assuming 3 reels
            first_symbol = reel_results_data[0]
            if not first_symbol.get('is_ton_prize') and \
               reel_results_data[1]['name'] == first_symbol['name'] and \
               reel_results_data[2]['name'] == first_symbol['name']:
                
                won_item_data_spec = first_symbol # This is the spec of the matched NFT
                db_nft = db.query(NFT).filter(NFT.name == won_item_data_spec['name']).first()
                if db_nft:
                    actual_val = Decimal(str(db_nft.floor_price))
                    inv_item = InventoryItem(
                        user_id=uid,
                        nft_id=db_nft.id,
                        current_value=float(actual_val),
                        variant=None, # Slots usually don't have variants unless designed for it
                        is_ton_prize=False
                    )
                    db.add(inv_item); db.flush() # Get item.id
                    
                    won_prizes_from_slot.append({
                        "id": inv_item.id,
                        "name": db_nft.name,
                        "imageFilename": db_nft.image_filename,
                        "floorPrice": float(db_nft.floor_price),
                        "currentValue": inv_item.current_value,
                        "is_ton_prize": False,
                        "variant": inv_item.variant
                    })
                    total_value_this_spin += actual_val # Added to total_won_ton, not directly to balance
                else:
                    logger.error(f"Slot win: NFT {won_item_data_spec['name']} not found in DB!")

        user.total_won_ton = float(Decimal(str(user.total_won_ton)) + total_value_this_spin)
        db.commit()
        return jsonify({"status":"success","reel_results":reel_results_data,"won_prizes":won_prizes_from_slot,"new_balance_ton":user.ton_balance})
    except Exception as e:
        db.rollback(); logger.error(f"Error in spin_slot: {e}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
    finally: db.close()


@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id'); mult_str = data.get('multiplier_str')
    if not all([iid, mult_str]): return jsonify({"error": "Missing params"}), 400
    try: mult = Decimal(mult_str); iid_int = int(iid)
    except: return jsonify({"error": "Invalid data format for multiplier or item ID"}), 400
    
    chances = {Decimal("1.5"):50,Decimal("2.0"):35,Decimal("3.0"):25,Decimal("5.0"):15,Decimal("10.0"):8,Decimal("20.0"):3}
    if mult not in chances: return jsonify({"error": "Invalid multiplier value"}), 400
    
    db = next(get_db())
    try:
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_int, InventoryItem.user_id == uid).with_for_update().first()
        if not item or item.is_ton_prize: return jsonify({"error": "Item not found or cannot be upgraded"}), 404
        
        user = db.query(User).filter(User.id == uid).with_for_update().first() # Also lock user for total_won_ton update
        if not user: return jsonify({"error": "User not found"}), 404 # Should not happen if item was found

        if random.uniform(0,100) < chances[mult]: # Success
            orig_val = Decimal(str(item.current_value)); 
            new_val = (orig_val * mult).quantize(Decimal('0.01'), ROUND_HALF_UP)
            increase_in_value = new_val - orig_val
            
            item.current_value = float(new_val)
            item.upgrade_multiplier = float(Decimal(str(item.upgrade_multiplier)) * mult) # Accumulate multiplier
            
            user.total_won_ton = float(Decimal(str(user.total_won_ton)) + increase_in_value) # Add only the increase
            db.commit()
            return jsonify({
                "status":"success",
                "message":f"Upgrade successful! New value: {new_val:.2f} TON",
                "item":{
                    "id":item.id,
                    "currentValue":item.current_value,
                    "name":item.nft.name,
                    "imageFilename":item.nft.image_filename,
                    "upgradeMultiplier":item.upgrade_multiplier,
                    "variant":item.variant 
                }
            })
        else: # Failure
            name_lost = item.nft.name; 
            value_lost = Decimal(str(item.current_value))
            
            user.total_won_ton = float(Decimal(str(user.total_won_ton)) - value_lost) # Subtract full value of lost item
            db.delete(item); 
            db.commit()
            return jsonify({"status":"failed","message":f"Upgrade failed! Lost {name_lost}.","item_lost":True})
    except Exception as e:
        db.rollback(); logger.error(f"Error in upgrade_item: {e}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
    finally: db.close()

@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid_convert = data.get('inventory_item_id')
    if not iid_convert: return jsonify({"error": "inventory_item_id required"}), 400
    try: iid_convert_int = int(iid_convert)
    except ValueError: return jsonify({"error": "Invalid inventory_item_id format"}), 400
    
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        item = db.query(InventoryItem).filter(InventoryItem.id == iid_convert_int, InventoryItem.user_id == uid).first() # Don't lock item, it will be deleted
        
        if not user: return jsonify({"error": "User not found"}), 404 # Should not happen if auth passed
        if not item: return jsonify({"error": "Item not found in your inventory"}), 404
        if item.is_ton_prize: return jsonify({"error": "Cannot convert a TON prize item."}), 400
            
        val_to_add = Decimal(str(item.current_value)); 
        user.ton_balance = float(Decimal(str(user.ton_balance)) + val_to_add)
        
        item_name_converted = item.nft.name if item.nft else item.item_name_override
        db.delete(item); 
        db.commit()
        return jsonify({
            "status":"success",
            "message":f"Item '{item_name_converted}' sold for {val_to_add:.2f} TON.",
            "new_balance_ton":user.ton_balance
        })
    except Exception as e:
        db.rollback(); logger.error(f"Error in convert_to_ton: {e}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
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
        if not items_to_sell: return jsonify({"status":"no_items","message":"No sellable items in your collection."})
            
        total_value_from_sell = sum(Decimal(str(i_sell.current_value)) for i_sell in items_to_sell)
        user.ton_balance = float(Decimal(str(user.ton_balance)) + total_value_from_sell)
        
        num_items_sold = len(items_to_sell)
        for i_del in items_to_sell: db.delete(i_del)
        
        db.commit()
        return jsonify({
            "status":"success",
            "message":f"All {num_items_sold} sellable items sold for a total of {total_value_from_sell:.2f} TON.",
            "new_balance_ton":user.ton_balance
        })
    except Exception as e:
        db.rollback(); logger.error(f"Error in sell_all_items: {e}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
    finally: db.close()

@app.route('/api/initiate_deposit', methods=['POST'])
def initiate_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); amt_str = data.get('amount')
    if amt_str is None: return jsonify({"error": "Amount required"}), 400
    try: orig_amt = float(amt_str)
    except ValueError: return jsonify({"error": "Invalid amount format"}), 400
    if not (0.1 <= orig_amt <= 10000): return jsonify({"error": "Amount must be between 0.1 and 10000 TON"}), 400
    
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).first() # No lock needed for user here
        if not user: return jsonify({"error": "User not found"}), 404
        
        # Check for existing active deposit
        if db.query(PendingDeposit).filter(PendingDeposit.user_id == uid, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first():
            return jsonify({"error": "You already have an active deposit. Please wait for it to expire or complete."}), 409
            
        nano_part = random.randint(10000, 999999); # Unique part in nanoTON
        final_nano_amt = int(orig_amt * 1e9) + nano_part
        
        pdep = PendingDeposit(
            user_id=uid,
            original_amount_ton=orig_amt,
            unique_identifier_nano_ton=nano_part,
            final_amount_nano_ton=final_nano_amt,
            expected_comment=DEPOSIT_COMMENT,
            expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES)
        )
        db.add(pdep); db.commit(); db.refresh(pdep)
        
        amount_to_send_display = f"{final_nano_amt / 1e9:.9f}".rstrip('0').rstrip('.') # For display
        
        return jsonify({
            "status":"success",
            "pending_deposit_id":pdep.id,
            "recipient_address":DEPOSIT_RECIPIENT_ADDRESS_RAW,
            "amount_to_send":amount_to_send_display, # User-friendly display string
            "final_amount_nano_ton":final_nano_amt,  # Exact nanoTON for ton://transfer
            "comment":DEPOSIT_COMMENT,
            "expires_at":pdep.expires_at.isoformat()
        })
    except Exception as e:
        db.rollback(); logger.error(f"Error in initiate_deposit: {e}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
    finally: db.close()

async def check_blockchain_for_deposit(pdep: PendingDeposit, db_sess: sessionmaker): # Pass session, not user
    prov = None
    try:
        prov = LiteBalancer.from_mainnet_config(trust_level=2); await prov.start_up()
        # Get transactions for the recipient address
        txs = await prov.get_transactions(DEPOSIT_RECIPIENT_ADDRESS_RAW, count=30) # Check recent transactions
        
        for tx in txs:
            # Check if it's an incoming internal message, matches the exact nanoTON amount,
            # and transaction time is after (or slightly before) deposit creation.
            if tx.in_msg and tx.in_msg.is_internal and \
               tx.in_msg.info.value_coins == pdep.final_amount_nano_ton and \
               tx.now > int((pdep.created_at - timedelta(minutes=5)).timestamp()): # Allow 5 min buffer for clock sync

                # Check comment
                cmt_slice = tx.in_msg.body.begin_parse()
                if cmt_slice.remaining_bits >= 32 and cmt_slice.load_uint(32) == 0: # Text comment prefix
                    try:
                        comment_text = cmt_slice.load_snake_string()
                        if comment_text == pdep.expected_comment:
                            # Transaction found and comment matches!
                            usr = db_sess.query(User).filter(User.id == pdep.user_id).with_for_update().first()
                            if not usr:
                                pdep.status = 'failed_user_not_found' # Or some other error status
                                db_sess.commit()
                                return {"status":"error","message":"User for deposit not found."}
                            
                            usr.ton_balance = float(Decimal(str(usr.ton_balance)) + Decimal(str(pdep.original_amount_ton)))
                            
                            # Handle referral bonus if applicable
                            if usr.referred_by_id:
                                referrer = db_sess.query(User).filter(User.id == usr.referred_by_id).with_for_update().first()
                                if referrer:
                                    referral_bonus = (Decimal(str(pdep.original_amount_ton)) * Decimal('0.10')).quantize(Decimal('0.01'),ROUND_HALF_UP)
                                    referrer.referral_earnings_pending = float(Decimal(str(referrer.referral_earnings_pending)) + referral_bonus)
                            
                            pdep.status = 'completed'
                            db_sess.commit()
                            return {"status":"success","message":"Deposit confirmed and credited!","new_balance_ton":usr.ton_balance}
                    except Exception as e_comment:
                        logger.debug(f"Comment parse error for tx {tx.hash.hex()}: {e_comment}")
                        # Continue, this tx might not be the one or comment format is unexpected

        # If no matching transaction found yet
        if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending':
            pdep.status = 'expired'
            db_sess.commit()
            return {"status":"expired","message":"Deposit request has expired."}
            
        return {"status":"pending","message":"Transaction not confirmed yet. Please wait or check again."}
    except Exception as e_bc_check:
        logger.error(f"Blockchain check error for deposit {pdep.id}: {e_bc_check}", exc_info=True)
        return {"status":"error","message":"Error during blockchain verification."}
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
        if not pdep: return jsonify({"error": "Pending deposit not found or does not belong to you"}), 404
        
        if pdep.status == 'completed':
            usr = db.query(User).filter(User.id == uid).first()
            return jsonify({"status":"success","message":"Deposit was already confirmed.","new_balance_ton":usr.ton_balance if usr else 0})
        
        if pdep.status == 'expired' or pdep.expires_at <= dt.now(timezone.utc):
            if pdep.status == 'pending': # Mark as expired if not already
                pdep.status = 'expired'; db.commit() 
            return jsonify({"status":"expired","message":"This deposit request has expired."}), 400 # Use 400 or other appropriate code
            
        # If still pending and not expired, try to check blockchain
        result = {}
        try:
            # Ensure a new event loop is used if called from a context that might already have one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
            loop.close()
        except Exception as e_async_exec:
            logger.error(f"Async execution error in verify_deposit for {pid}: {e_async_exec}", exc_info=True)
            return jsonify({"status":"error","message":"Server error during verification process."}), 500
            
        return jsonify(result)
        
    except Exception as e_outer:
        db.rollback(); # Rollback if any outer error
        logger.error(f"Outer error in verify_deposit for {pid}: {e_outer}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
    finally: db.close()

@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    # No auth needed for public leaderboard
    db = next(get_db())
    try:
        leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
        return jsonify([
            {
                "rank": r_idx + 1,
                "name": u_leader.first_name or u_leader.username or f"User_{str(u_leader.id)[:6]}",
                "avatarChar": (u_leader.first_name or u_leader.username or "U")[0].upper(),
                "income": u_leader.total_won_ton,
                "user_id": u_leader.id
            } for r_idx, u_leader in enumerate(leaders)
        ])
    except Exception as e:
        logger.error(f"Error in get_leaderboard: {e}", exc_info=True)
        return jsonify({"error":"Could not load leaderboard due to a server error."}), 500
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
            user.referral_earnings_pending = 0.0; 
            db.commit()
            return jsonify({
                "status":"success",
                "message":f"{withdrawn_amount:.2f} TON referral earnings withdrawn to main balance.",
                "new_balance_ton":user.ton_balance,
                "new_referral_earnings_pending":0.0
            })
        else: 
            return jsonify({"status":"no_earnings","message":"No referral earnings to withdraw."})
    except Exception as e:
        db.rollback(); logger.error(f"Error withdrawing referral earnings for {uid}: {e}", exc_info=True)
        return jsonify({"error": "DB error or unexpected issue."}), 500
    finally: db.close()

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]
    data = flask_request.get_json()
    code_txt = data.get('promocode_text', "").strip()
    if not code_txt:
        return jsonify({"status":"error","message":"Promocode text cannot be empty."}), 400
    
    db = next(get_db())
    try:
        user = db.query(User).filter(User.id == uid).with_for_update().first()
        if not user:
            return jsonify({"status":"error","message":"User not found."}), 404 # Should be rare if auth passed
            
        promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).with_for_update().first()
        if not promo:
            return jsonify({"status":"error","message":"Invalid promocode."}), 404
            
        if promo.activations_left != -1 and promo.activations_left <= 0 : # -1 for unlimited
            return jsonify({"status":"error","message":"This promocode has no activations left."}), 400
            
        existing_redemption = db.query(UserPromoCodeRedemption).filter(
            UserPromoCodeRedemption.user_id == user.id,
            UserPromoCodeRedemption.promo_code_id == promo.id
        ).first()
        if existing_redemption:
            return jsonify({"status":"error","message":"You have already redeemed this promocode."}), 400
            
        if promo.activations_left != -1: # Don't decrement if -1 (unlimited)
            promo.activations_left -= 1
        
        user.ton_balance = float(Decimal(str(user.ton_balance)) + Decimal(str(promo.ton_amount)))
        
        new_redemption = UserPromoCodeRedemption(user_id=user.id, promo_code_id=promo.id)
        db.add(new_redemption)
        db.commit()
        
        return jsonify({
            "status":"success",
            "message":f"Promocode '{code_txt}' redeemed successfully! You received {promo.ton_amount:.2f} TON.",
            "new_balance_ton":user.ton_balance
        })
    except IntegrityError as ie: 
        db.rollback()
        logger.error(f"IntegrityError redeeming promocode '{code_txt}' for user {uid}: {ie}", exc_info=True)
        return jsonify({"status":"error","message":"Promocode redemption failed due to a conflict. Please try again."}), 409 # Conflict
    except Exception as e:
        db.rollback()
        logger.error(f"Error redeeming promocode '{code_txt}' for user {uid}: {e}", exc_info=True)
        return jsonify({"status":"error","message":"A server error occurred during promocode redemption."}), 500
    finally:
        db.close()

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST'])
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"status":"error","message":"Auth failed"}), 401
    player_user_id = auth_user_data["id"]

    if not TONNEL_SENDER_INIT_DATA: 
        logger.error("Tonnel withdrawal attempt: TONNEL_SENDER_INIT_DATA not set.")
        return jsonify({"status":"error","message":"Withdrawal service is currently unavailable."}), 500
        
    db = next(get_db())
    try:
        item_to_withdraw = db.query(InventoryItem).filter(
            InventoryItem.id == inventory_item_id, 
            InventoryItem.user_id == player_user_id
        ).with_for_update(of=InventoryItem).first() # Lock the item row

        if not item_to_withdraw:
            return jsonify({"status":"error","message":"Item not found in your inventory."}), 404
        if item_to_withdraw.is_ton_prize:
            return jsonify({"status":"error","message":"TON prizes cannot be withdrawn this way."}), 400
            
        item_name_for_tonnel = item_to_withdraw.nft.name if item_to_withdraw.nft else item_to_withdraw.item_name_override
        if not item_name_for_tonnel: # Should not happen if data is consistent
            logger.error(f"Item {inventory_item_id} has no name for Tonnel withdrawal.")
            return jsonify({"status":"error","message":"Item data inconsistent, cannot withdraw."}), 500

        tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret_passphrase=TONNEL_GIFT_SECRET)
        tonnel_result = {}
        
        # Run async Tonnel client in a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            tonnel_result = loop.run_until_complete(
                tonnel_client.send_gift_to_user(gift_item_name=item_name_for_tonnel, receiver_telegram_id=player_user_id)
            )
        finally:
            loop.close()

        if tonnel_result and tonnel_result.get("status") == "success":
            # Deduct value from total_won_ton as it's no longer "won value in game"
            value_deducted_from_winnings = Decimal(str(item_to_withdraw.current_value))
            player = db.query(User).filter(User.id == player_user_id).with_for_update(of=User).first() # Lock user
            if player:
                player.total_won_ton = float(max(Decimal('0'), Decimal(str(player.total_won_ton)) - value_deducted_from_winnings)) 
            
            db.delete(item_to_withdraw)
            db.commit()
            return jsonify({
                "status":"success",
                "message":f"Gift '{item_name_for_tonnel}' sent via Tonnel! {tonnel_result.get('message', '')}",
                "details":tonnel_result.get("details")
            })
        else:
            db.rollback() # Important: rollback if Tonnel transfer failed
            logger.error(f"Tonnel withdrawal failed for item {inventory_item_id}, user {player_user_id}. Response: {tonnel_result}")
            return jsonify({"status":"error","message":f"Tonnel withdrawal failed: {tonnel_result.get('message', 'Tonnel API communication error')}"}), 500
            
    except Exception as e:
        db.rollback()
        logger.error(f"Tonnel withdrawal wrapper exception for item {inventory_item_id}, user {player_user_id}: {e}", exc_info=True)
        return jsonify({"status":"error","message":"An unexpected server error occurred during withdrawal."}), 500
    finally: 
        db.close()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # When deployed, Gunicorn or similar will be used. This is for local dev.
    # use_reloader=True is fine for local dev, False for production via WSGI server.
    # debug=False is generally better for anything beyond very early dev.
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=True)
