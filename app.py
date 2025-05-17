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

# Pytoniq imports
from pytoniq import LiteBalancer
import asyncio

load_dotenv()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MINI_APP_NAME = os.environ.get("MINI_APP_NAME", "case")
MINI_APP_URL = os.environ.get("MINI_APP_URL", f"https://t.me/caseKviBot/{MINI_APP_NAME}")
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
        self.secret = gift_secret
        self.authdata = sender_auth_data
        self._crypto_js = require("crypto-js")
        self._session_instance = None

    async def _get_session(self):
        if self._session_instance is None or self._session_instance.closed:
            self._session_instance = AsyncSession(impersonate="chrome110", http_version=2)
        return self._session_instance

    async def _close_session_if_open(self):
        if self._session_instance and not self._session_instance.closed:
            await self._session_instance.close()
            self._session_instance = None

    async def _make_request(self, method, url, headers=None, json_payload=None, timeout=30):
        session = await self._get_session()
        try:
            response = None # Initialize response
            if method.upper() == "GET":
                response = await session.get(url, headers=headers, timeout=timeout)
            elif method.upper() == "POST":
                response = await session.post(url, headers=headers, json=json_payload, timeout=timeout)
            elif method.upper() == "OPTIONS":
                response = await session.options(url, headers=headers, timeout=timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            logger.debug(f"Tonnel API {method} {url} - Status: {response.status_code}")
            if method.upper() != "OPTIONS": # Don't raise for OPTIONS by default
                response.raise_for_status()
            
            if response.status_code == 204: return None
            if method.upper() == "OPTIONS" and response.status_code // 100 == 2 : return {"status": "options_ok"}
            return response.json()
        except Exception as e:
            logger.error(f"Tonnel API request error ({method} {url}): {type(e).__name__} - {e}", exc_info=False)
            if response is not None: # Check if response object exists
                try: logger.error(f"Response content for error: {await response.text()}")
                except: pass
            raise

    async def send_gift_to_user(self, gift_item_name: str, receiver_telegram_id: int):
        logger.info(f"Attempting Tonnel gift '{gift_item_name}' to user {receiver_telegram_id} using sender auth: {self.authdata[:30]}...")
        if not self.authdata:
            logger.error("TONNEL_SENDER_INIT_DATA not configured.")
            return {"status": "error", "message": "Tonnel sender not configured."}

        try:
            await self._make_request("GET", "https://marketplace.tonnel.network/")
            logger.info("Tonnel: Initial GET to marketplace.tonnel.network done.")
            
            filter_str = json.dumps({"price":{"$exists":True},"refunded":{"$ne":True},"buyer":{"$exists":False},"export_at":{"$exists":True},"gift_name":gift_item_name,"asset":"TON"})
            page_gifts_payload = {"filter": filter_str, "limit":10, "page":1, "sort":'{"price":1,"gift_id":-1}'}
            pg_headers = {"Content-Type": "application/json", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            
            await self._make_request("OPTIONS", "https://gifts2.tonnel.network/api/pageGifts", headers={"Access-Control-Request-Method":"POST", "Access-Control-Request-Headers":"content-type", "Origin":"https://tonnel-gift.vercel.app", "Referer":"https://tonnel-gift.vercel.app/"})
            gifts_found = await self._make_request("POST", "https://gifts2.tonnel.network/api/pageGifts", headers=pg_headers, json_payload=page_gifts_payload)
            
            if not gifts_found or not isinstance(gifts_found, list) or len(gifts_found) == 0:
                logger.warning(f"Tonnel: No gifts found for '{gift_item_name}'. Resp: {gifts_found}")
                return {"status": "error", "message": f"No '{gift_item_name}' gifts on Tonnel."}
            low_gift = gifts_found[0]
            logger.info(f"Tonnel: Found gift for '{gift_item_name}': ID {low_gift.get('gift_id')}, Price {low_gift.get('price')} TON")

            user_info_payload = {"authData": self.authdata, "user": receiver_telegram_id}
            ui_headers = {"Content-Type": "application/json", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/"}
            await self._make_request("OPTIONS", "https://gifts2.tonnel.network/api/userInfo", headers={"Access-Control-Request-Method":"POST", "Access-Control-Request-Headers":"content-type", "Origin":"https://marketplace.tonnel.network", "Referer":"https://marketplace.tonnel.network/"})
            user_check_resp = await self._make_request("POST", "https://gifts2.tonnel.network/api/userInfo", headers=ui_headers, json_payload=user_info_payload)
            logger.info(f"Tonnel: UserInfo check response: {user_check_resp}")
            if not user_check_resp or user_check_resp.get("status") != "success":
                logger.warning(f"Tonnel: UserInfo check failed for receiver {receiver_telegram_id}. Resp: {user_check_resp}")
                return {"status": "error", "message": f"Tonnel user check failed. {user_check_resp.get('message', '')}"}

            time_now_ts = int(time.time())
            encrypted_ts = self._crypto_js.AES.encrypt(f"{time_now_ts}", self.secret).toString()
            buy_gift_url = f"https://gifts.coffin.meme/api/buyGift/{low_gift['gift_id']}"
            buy_payload = {"anonymously": True, "asset": "TON", "authData": self.authdata, "price": low_gift['price'], "receiver": receiver_telegram_id, "showPrice": False, "timestamp": encrypted_ts}
            buy_headers = {"Content-Type": "application/json", "Origin": "https://marketplace.tonnel.network", "Referer": "https://marketplace.tonnel.network/", "Host":"gifts.coffin.meme"}
            
            await self._make_request("OPTIONS", buy_gift_url, headers={"Access-Control-Request-Method":"POST", "Access-Control-Request-Headers":"content-type", "Origin":"https://marketplace.tonnel.network", "Referer":"https://marketplace.tonnel.network/"})
            purchase_resp = await self._make_request("POST", buy_gift_url, headers=buy_headers, json_payload=buy_payload, timeout=90)
            logger.info(f"Tonnel: BuyGift response for {low_gift['gift_id']} to {receiver_telegram_id}: {purchase_resp}")

            if purchase_resp and purchase_resp.get("status") == "success":
                logger.info(f"Tonnel: Gift '{gift_item_name}' to user {receiver_telegram_id} success.")
                return {"status": "success", "message": f"Gift '{gift_item_name}' sent!", "details": purchase_resp}
            else:
                logger.error(f"Tonnel: Failed to send gift '{gift_item_name}'. Resp: {purchase_resp}")
                return {"status": "error", "message": f"Tonnel transfer failed. {purchase_resp.get('message', '')}"}
        except Exception as e:
            logger.error(f"Tonnel: Error sending gift '{gift_item_name}' to {receiver_telegram_id}: {e}", exc_info=True)
            return {"status": "error", "message": f"Unexpected error during Tonnel withdrawal: {str(e)}"}
        finally:
            await self._close_session_if_open()


# --- Utility and Data Functions ---
def generate_image_filename_from_name(name_str: str) -> str:
    if not name_str: return 'placeholder.png';
    if name_str == "Durov's Cap": return "Durov's-Cap.png";
    if name_str == "Vintage Cigar": return "Vintage-CIgar.png";
    name_str_rep = name_str.replace('-', '_');
    if name_str_rep in ['Amber', 'Midnight_Blue', 'Onyx_Black', 'Black']: return name_str_rep + '.png';
    cleaned = re.sub(r'\s+', '-', name_str.replace('&', 'and').replace("'", ""));
    return re.sub(r'-+', '-', cleaned) + '.png';

UPDATED_FLOOR_PRICES = { 'Plush Pepe': 1200.0, 'Neko Helmet': 15.0, 'Sharp Tongue': 17.0, "Durov's Cap": 251.0, 'Voodoo Doll': 9.4, 'Vintage Cigar': 19.7, 'Astral Shard': 50.0, 'Scared Cat': 22.0, 'Swiss Watch': 18.6, 'Perfume Bottle': 38.3, 'Precious Peach': 100.0, 'Toy Bear': 16.3, 'Genie Lamp': 19.3, 'Loot Bag': 25.0, 'Kissed Frog': 14.8, 'Electric Skull': 10.9, 'Diamond Ring': 8.06, 'Mini Oscar': 40.5, 'Party Sparkler': 2.0, 'Homemade Cake': 2.0, 'Cookie Heart': 1.8, 'Jack-in-the-box': 2.0, 'Skull Flower': 3.4, 'Lol Pop': 1.4, 'Hynpo Lollipop': 1.4, 'Desk Calendar': 1.4, 'B-Day Candle': 1.4, 'Record Player': 4.0, 'Jelly Bunny': 3.6, 'Tama Gadget': 4.0, 'Snow Globe': 4.0, 'Eternal Rose': 11.0, 'Love Potion': 5.4, 'Top Hat': 6.0 }

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

for case_info in cases_data_backend:
    ev = sum(p['floorPrice'] * p['probability'] * (2.5 if case_info['id'] == 'black' else 1) for p in case_info['prizes'])
    actual_rtp = (ev / case_info['priceTON']) * 100 if case_info['priceTON'] > 0 else float('inf')
    logger.info(f"Case (fixed price check): {case_info['name']}, Price: {case_info['priceTON']:.2f} TON, EV: {ev:.2f}, Actual RTP: {actual_rtp:.2f}%")

def populate_initial_data():
    db = SessionLocal();
    try:
        existing_nfts = {nft.name: nft for nft in db.query(NFT).all()}
        nfts_to_add = []
        updated_count = 0
        for name, price in UPDATED_FLOOR_PRICES.items():
            if name in existing_nfts:
                if existing_nfts[name].floor_price != price: existing_nfts[name].floor_price = price; updated_count += 1
            else: nfts_to_add.append(NFT(name=name, image_filename=generate_image_filename_from_name(name), floor_price=price))
        if nfts_to_add: db.add_all(nfts_to_add)
        db.commit(); logger.info(f"NFTs: {len(nfts_to_add)} new, {updated_count} updated.")
        if not db.query(PromoCode).filter(PromoCode.code_text == 'durov').first(): db.add(PromoCode(code_text='durov', activations_left=10, ton_amount=5.0)); db.commit(); logger.info("Promocode 'durov' seeded.")
    except Exception as e: db.rollback(); logger.error(f"Populate data error: {e}", exc_info=True)
    finally: db.close()
populate_initial_data()

DEPOSIT_RECIPIENT_ADDRESS_RAW = "UQBZs1e2h5CwmxQxmAJLGNqEPcQ9iU3BCDj0NSzbwTiGa3hR"; DEPOSIT_COMMENT = "cpd7r07ud3s"; PENDING_DEPOSIT_EXPIRY_MINUTES = 30
app = Flask(__name__); CORS(app, resources={r"/api/*": {"origins": ["https://vasiliy-katsyka.github.io"]}})
if not BOT_TOKEN: logger.error("BOT_TOKEN not found!"); exit("BOT_TOKEN is not set.")
bot = telebot.TeleBot(BOT_TOKEN)
def get_db(): db = SessionLocal();_ = (yield db);db.close() # Simplified generator
def validate_init_data(init_data_str: str, bot_token: str) -> dict | None:
    try:
        if not init_data_str: return None
        parsed_data = dict(parse_qs(init_data_str))
        if not all(k in parsed_data for k in ['hash', 'user', 'auth_date']): return None
        hash_received = parsed_data.pop('hash')[0]; auth_date_ts = int(parsed_data['auth_date'][0])
        if (int(dt.now(timezone.utc).timestamp()) - auth_date_ts) > AUTH_DATE_MAX_AGE_SECONDS: logger.warning("initData expired"); return None
        data_check_string = "\n".join(f"{k}={parsed_data[k][0]}" for k in sorted(parsed_data.keys()))
        secret_key = hmac.new("WebAppData".encode(), bot_token.encode(), hashlib.sha256).digest()
        calculated_hash_hex = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if calculated_hash_hex == hash_received:
            user_info_dict = json.loads(unquote(parsed_data['user'][0]))
            if 'id' not in user_info_dict: logger.warning("ID missing in user_info_dict"); return None
            return { "id": int(user_info_dict["id"]), **user_info_dict }
        logger.warning("Hash mismatch in initData validation"); return None
    except Exception as e: logger.error(f"initData validation error: {e}", exc_info=True); return None

# --- API Endpoints ---
@app.route('/')
def index_route(): return "Pusik Gifts App is Running!"

@app.route('/api/get_user_data', methods=['POST'])
def get_user_data_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db()); user = db.query(User).filter(User.id == uid).first()
    if not user: user = User(id=uid, username=auth.get("username"), first_name=auth.get("first_name"), last_name=auth.get("last_name"), referral_code=f"ref_{uid}_{random.randint(1000,9999)}"); db.add(user); db.commit(); db.refresh(user)
    inv = [{"id": i.id, "name": i.nft.name, "imageFilename": i.nft.image_filename, "floorPrice": i.nft.floor_price, "currentValue": i.current_value, "upgradeMultiplier": i.upgrade_multiplier, "variant": i.variant, "obtained_at": i.obtained_at.isoformat() if i.obtained_at else None} for i in user.inventory]
    refs = db.query(User).filter(User.referred_by_id == uid).count()
    return jsonify({"id": user.id, "username": user.username, "first_name": user.first_name, "last_name": user.last_name, "tonBalance": user.ton_balance, "starBalance": user.star_balance, "inventory": inv, "referralCode": user.referral_code, "referralEarningsPending": user.referral_earnings_pending, "total_won_ton": user.total_won_ton, "invited_friends_count": refs})

@app.route('/api/open_case', methods=['POST'])
def open_case_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); cid = data.get('case_id')
    if not cid: return jsonify({"error": "case_id required"}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == uid).first()
    if not user: return jsonify({"error": "User not found"}), 404
    tcase = next((c for c in cases_data_backend if c['id'] == cid), None)
    if not tcase: return jsonify({"error": "Case not found"}), 404
    cost = tcase['priceTON']
    if user.ton_balance < cost: return jsonify({"error": f"Not enough TON. Need {cost:.2f}"}), 400
    
    prizes = tcase['prizes']; rv = random.random(); cprob = 0; chosen_prize_info = None
    for p_info in prizes: # Use p_info to avoid conflict if 'p' is used elsewhere
        cprob += p_info['probability']
        if rv <= cprob:
            chosen_prize_info = p_info # Indented correctly
            break                     # Indented correctly
            
    if not chosen_prize_info: chosen_prize_info = random.choice(prizes) # Fallback
    
    user.ton_balance -= cost
    dbnft = db.query(NFT).filter(NFT.name == chosen_prize_info['name']).first()
    if not dbnft: user.ton_balance += cost; db.commit(); logger.error(f"CRITICAL: NFT {chosen_prize_info['name']} not found in DB!"); return jsonify({"error": "Prize NFT missing from DB"}), 500
    
    item_variant = "black_singularity" if tcase['id'] == 'black' else None
    actual_item_value = dbnft.floor_price * (2.5 if item_variant == "black_singularity" else 1)
    user.total_won_ton += actual_item_value
    
    new_item = InventoryItem(user_id=uid, nft_id=dbnft.id, current_value=round(actual_item_value, 2), variant=item_variant)
    db.add(new_item); db.commit(); db.refresh(new_item)
    return jsonify({"status": "success", "won_prize": {"id": new_item.id, "name": dbnft.name, "imageFilename": dbnft.image_filename, "floorPrice": dbnft.floor_price, "currentValue": new_item.current_value, "variant": new_item.variant}, "new_balance_ton": user.ton_balance})

@app.route('/api/upgrade_item', methods=['POST'])
def upgrade_item_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id'); mult_str = data.get('multiplier_str')
    if not all([iid, mult_str]): return jsonify({"error": "Missing params"}), 400
    try: mult = float(mult_str); iid = int(iid)
    except ValueError: return jsonify({"error": "Invalid data"}), 400
    chances = {1.5:50, 2.0:35, 3.0:25, 5.0:15, 10.0:8, 20.0:3}
    if mult not in chances: return jsonify({"error": "Invalid multiplier"}), 400
    db = next(get_db()); item = db.query(InventoryItem).filter(InventoryItem.id == iid, InventoryItem.user_id == uid).first()
    if not item: return jsonify({"error": "Item not found"}), 404
    user = db.query(User).filter(User.id == uid).first()
    if random.uniform(0,100) < chances[mult]:
        orig_val = item.current_value; new_val = round(orig_val * mult, 2); increase = new_val - orig_val
        item.current_value = new_val; item.upgrade_multiplier *= mult
        if user: user.total_won_ton += increase
        db.commit(); return jsonify({"status": "success", "message": f"Upgraded! New: {new_val:.2f} TON", "item": {"id": item.id, "currentValue": new_val, "name": item.nft.name, "upgradeMultiplier": item.upgrade_multiplier, "variant": item.variant }})
    else:
        name_lost = item.nft.name; val_lost = item.current_value
        if user: user.total_won_ton -= val_lost
        db.delete(item); db.commit(); return jsonify({"status": "failed", "message": f"Failed! Lost {name_lost}.", "item_lost": True})

@app.route('/api/convert_to_ton', methods=['POST'])
def convert_to_ton_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); iid = data.get('inventory_item_id')
    if not iid: return jsonify({"error": "ID required"}), 400;
    try: iid = int(iid)
    except ValueError: return jsonify({"error": "Invalid ID"}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == uid).first(); item = db.query(InventoryItem).filter(InventoryItem.id == iid, InventoryItem.user_id == uid).first()
    if not user or not item: return jsonify({"error": "User/item not found"}), 404
    val = item.current_value; user.ton_balance += val; db.delete(item); db.commit()
    return jsonify({"status": "success", "message": f"{item.nft.name} sold for {val:.2f} TON.", "new_balance_ton": user.ton_balance})

@app.route('/api/sell_all_items', methods=['POST'])
def sell_all_items_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db()); user = db.query(User).filter(User.id == uid).first()
    if not user: return jsonify({"error": "User not found"}), 404
    if not user.inventory: return jsonify({"status": "no_items", "message": "Inventory empty."})
    total_val = sum(i.current_value for i in user.inventory); user.ton_balance += total_val
    for i in list(user.inventory): db.delete(i)
    db.commit(); return jsonify({"status": "success", "message": f"All sold for {total_val:.2f} TON.", "new_balance_ton": user.ton_balance})

@app.route('/api/initiate_deposit', methods=['POST'])
def initiate_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); amt_str = data.get('amount')
    if amt_str is None: return jsonify({"error": "Amount required"}), 400
    try: orig_amt = float(amt_str)
    except ValueError: return jsonify({"error": "Invalid amount"}), 400
    if not (0 < orig_amt <= 10000): return jsonify({"error": "Amount out of range"}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == uid).first()
    if not user: return jsonify({"error": "User not found"}), 404
    nano_part = random.randint(10000, 999999); final_nano_amt = int(orig_amt * 1e9) + nano_part
    if db.query(PendingDeposit).filter(PendingDeposit.user_id == uid, PendingDeposit.status == 'pending', PendingDeposit.expires_at > dt.now(timezone.utc)).first(): return jsonify({"error": "Active deposit exists."}), 409
    pdep = PendingDeposit(user_id=uid, original_amount_ton=orig_amt, unique_identifier_nano_ton=nano_part, final_amount_nano_ton=final_nano_amt, expected_comment=DEPOSIT_COMMENT, expires_at=dt.now(timezone.utc) + timedelta(minutes=PENDING_DEPOSIT_EXPIRY_MINUTES))
    db.add(pdep); db.commit(); db.refresh(pdep)
    disp_amt = f"{final_nano_amt / 1e9:.9f}".rstrip('0').rstrip('.');
    return jsonify({"status": "success", "pending_deposit_id": pdep.id, "recipient_address": DEPOSIT_RECIPIENT_ADDRESS_RAW, "amount_to_send": disp_amt, "final_amount_nano_ton": final_nano_amt, "comment": DEPOSIT_COMMENT, "expires_at": pdep.expires_at.isoformat()})

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
                        if cmt_slice.load_snake_string() == pdep.expected_comment:
                            usr = db_sess.query(User).filter(User.id == pdep.user_id).first()
                            if not usr: pdep.status = 'failed'; db_sess.commit(); return {"status": "error", "message": "User vanished"}
                            usr.ton_balance += pdep.original_amount_ton
                            if usr.referred_by_id:
                                ref = db_sess.query(User).filter(User.id == usr.referred_by_id).first()
                                if ref: ref.referral_earnings_pending += round(pdep.original_amount_ton * 0.10, 2)
                            pdep.status = 'completed'; db_sess.commit()
                            return {"status": "success", "message": "Deposit confirmed!", "new_balance_ton": usr.ton_balance}
                    except: pass
        if pdep.expires_at <= dt.now(timezone.utc) and pdep.status == 'pending': pdep.status = 'expired'; db_sess.commit(); return {"status": "expired", "message": "Deposit expired."}
        return {"status": "pending", "message": "Not confirmed."}
    except Exception as e: logger.error(f"BC check error: {e}", exc_info=True); return {"status": "error", "message": "Error checking tx."}
    finally:
        if prov: await prov.close_all()

@app.route('/api/verify_deposit', methods=['POST'])
def verify_deposit_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); pid = data.get('pending_deposit_id')
    if not pid: return jsonify({"error": "ID required"}), 400
    db = next(get_db()); pdep = db.query(PendingDeposit).filter(PendingDeposit.id == pid, PendingDeposit.user_id == uid).first()
    if not pdep: return jsonify({"error": "Deposit not found"}), 404
    if pdep.status == 'completed': usr = db.query(User).filter(User.id == uid).first(); return jsonify({"status": "success", "message": "Already confirmed.", "new_balance_ton": usr.ton_balance if usr else 0})
    if pdep.status == 'expired' or pdep.expires_at <= dt.now(timezone.utc):
        if pdep.status == 'pending': pdep.status = 'expired'; db.commit()
        return jsonify({"status": "expired", "message": "Deposit expired."}), 400
    
    result = {}
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is running (e.g. in Jupyter or another async context), use ensure_future
            # This part is tricky in plain Flask. For production, an async framework like Quart or FastAPI is better.
            # For now, we'll try to run it, but this might behave unexpectedly in some Flask/Gunicorn setups.
            logger.warning("Event loop already running. Attempting ensure_future for blockchain check.")
            # This won't block and wait in a sync Flask route, so the response might be premature.
            # Consider a separate worker/task queue for these calls in production.
            # For a simple demo, it might appear to work if the check is fast.
            future = asyncio.ensure_future(check_blockchain_for_deposit(pdep, db))
            # This will not wait for the future to complete in a sync route.
            # A better way for sync Flask is to use loop.run_until_complete if possible,
            # or structure this as a task to be polled.
            # For now, let's make it behave like the original run_until_complete for simplicity,
            # acknowledging its limitations in a sync server.
            result = loop.run_until_complete(future) if not future.done() else future.result()

        else: # If no loop is running, we can use run_until_complete
            result = loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
    except RuntimeError as e:
         if "cannot be called from a running event loop" in str(e) or "no current event loop" in str(e).lower():
            logger.warning(f"Asyncio loop issue: {e}. Creating new loop for this call.")
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            result = new_loop.run_until_complete(check_blockchain_for_deposit(pdep, db))
            # new_loop.close() # Closing might be problematic depending on context
         else:
            logger.error(f"RuntimeError during verify_deposit: {e}", exc_info=True)
            return jsonify({"status": "error", "message": "Internal error during verification."}), 500
    except Exception as e:
        logger.error(f"General exception during verify_deposit: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Unexpected error during verification."}), 500
        
    return jsonify(result)


@app.route('/api/get_leaderboard', methods=['GET'])
def get_leaderboard_api():
    db = next(get_db()); leaders = db.query(User).order_by(User.total_won_ton.desc()).limit(100).all()
    return jsonify([{"rank": r+1, "name": u.first_name or u.username or f"User_{str(u.id)[:6]}", "avatarChar": (u.first_name or u.username or "U")[0].upper(), "income": u.total_won_ton, "user_id": u.id} for r, u in enumerate(leaders)])

@app.route('/api/withdraw_referral_earnings', methods=['POST'])
def withdraw_referral_earnings_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; db = next(get_db()); user = db.query(User).filter(User.id == uid).first()
    if not user: return jsonify({"error": "User not found"}), 404
    if user.referral_earnings_pending > 0:
        withdrawn = user.referral_earnings_pending; user.ton_balance += withdrawn; user.referral_earnings_pending = 0.0; db.commit()
        return jsonify({"status": "success", "message": f"{withdrawn:.2f} TON withdrawn.", "new_balance_ton": user.ton_balance, "new_referral_earnings_pending": 0.0})
    return jsonify({"status": "no_earnings", "message": "No earnings."})

@app.route('/api/redeem_promocode', methods=['POST'])
def redeem_promocode_api():
    auth = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN);
    if not auth: return jsonify({"error": "Auth failed"}), 401
    uid = auth["id"]; data = flask_request.get_json(); code_txt = data.get('promocode_text', "").strip()
    if not code_txt: return jsonify({"status": "error", "message": "Code empty."}), 400
    db = next(get_db()); user = db.query(User).filter(User.id == uid).first(); promo = db.query(PromoCode).filter(PromoCode.code_text == code_txt).first()
    if not user: return jsonify({"status": "error", "message": "User not found."}), 404
    if not promo: return jsonify({"status": "error", "message": "Invalid code."}), 404
    if promo.activations_left <= 0: return jsonify({"status": "error", "message": "Code expired."}), 400
    promo.activations_left -= 1; user.ton_balance += promo.ton_amount
    try: db.commit(); return jsonify({"status": "success", "message": f"Code '{code_txt}' redeemed! +{promo.ton_amount:.2f} TON.", "new_balance_ton": user.ton_balance})
    except SQLAlchemyError: db.rollback(); return jsonify({"status": "error", "message": "DB error."}), 500

@app.route('/api/withdraw_item_via_tonnel/<int:inventory_item_id>', methods=['POST'])
def withdraw_item_via_tonnel_api_sync_wrapper(inventory_item_id):
    auth_user_data = validate_init_data(flask_request.headers.get('X-Telegram-Init-Data'), BOT_TOKEN)
    if not auth_user_data: return jsonify({"status": "error", "message": "Authentication failed"}), 401
    player_user_id = auth_user_data["id"]

    if not TONNEL_SENDER_INIT_DATA:
        logger.error("Tonnel withdrawal: TONNEL_SENDER_INIT_DATA not configured.")
        return jsonify({"status": "error", "message": "Withdrawal service not configured."}), 500

    db = next(get_db())
    item_to_withdraw = db.query(InventoryItem).filter(InventoryItem.id == inventory_item_id, InventoryItem.user_id == player_user_id).first()
    if not item_to_withdraw: return jsonify({"status": "error", "message": "Item not found or already withdrawn."}), 404
    item_name_for_tonnel = item_to_withdraw.nft.name
    
    tonnel_client = TonnelGiftSender(sender_auth_data=TONNEL_SENDER_INIT_DATA, gift_secret=TONNEL_GIFT_SECRET)
    
    tonnel_result = {}
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running(): # This check might not be sufficient in all WSGI server contexts
            logger.warning("Event loop seems to be running. Trying to use it for Tonnel withdrawal.")
            # This is a tricky part in synchronous Flask.
            # A better approach for production would be to offload this to a task queue (Celery, RQ)
            # or use an async framework (Quart, FastAPI).
            # For now, we attempt to run it, but it might block or behave unexpectedly under load.
            async def run_in_current_loop():
                return await tonnel_client.send_gift_to_user(
                    gift_item_name=item_name_for_tonnel,
                    receiver_telegram_id=player_user_id
                )
            # This creates a task but doesn't necessarily wait for it in a way that sync Flask likes.
            # For simplicity in this example, we'll try to run it to completion if possible.
            try:
                tonnel_result = loop.run_until_complete(run_in_current_loop())
            except RuntimeError as re: # If it's already running and run_until_complete fails
                 if "cannot be called from a running event loop" in str(re):
                    logger.error("Cannot run_until_complete in already running loop. Tonnel withdrawal might not complete in this request.")
                    return jsonify({"status":"pending_internal", "message":"Withdrawal processing, result will be updated later."}), 202 # Accepted
                 else: raise re
        else:
            tonnel_result = loop.run_until_complete(
                tonnel_client.send_gift_to_user(
                    gift_item_name=item_name_for_tonnel,
                    receiver_telegram_id=player_user_id
                )
            )

        if tonnel_result and tonnel_result.get("status") == "success":
            item_value_deducted = item_to_withdraw.current_value
            player_profile = db.query(User).filter(User.id == player_user_id).first()
            if player_profile: player_profile.total_won_ton = max(0, player_profile.total_won_ton - item_value_deducted)
            db.delete(item_to_withdraw); db.commit()
            return jsonify({"status": "success", "message": f"Gift '{item_name_for_tonnel}' sent via Tonnel! {tonnel_result.get('message', '')}", "details": tonnel_result.get("details")})
        else:
            return jsonify({"status": "error", "message": f"Tonnel withdrawal failed: {tonnel_result.get('message', 'Tonnel API error')}"}), 500
    except Exception as e:
        logger.error(f"Exception in Tonnel withdrawal wrapper for item {inventory_item_id}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Unexpected error during withdrawal."}), 500

# --- Telegram Bot Commands ---
@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"/start from {message.chat.id} ({message.from_user.username}) text: '{message.text}'")
    db = next(get_db()); uid = message.chat.id; tg_user = message.from_user
    user = db.query(User).filter(User.id == uid).first(); created = False
    if not user: created = True; user = User(id=uid, username=tg_user.username, first_name=tg_user.first_name, last_name=tg_user.last_name, referral_code=f"ref_{uid}_{random.randint(1000,9999)}"); db.add(user)
    try:
        parts = message.text.split(' ');
        if len(parts) > 1 and parts[1].startswith('startapp='):
            param_val = parts[1].split('=')[1]
            if param_val.startswith('ref_') and (created or not user.referred_by_id):
                ref_code = param_val; referrer = db.query(User).filter(User.referral_code == ref_code).first()
                if referrer and referrer.id != user.id: user.referred_by_id = referrer.id; logger.info(f"User {uid} ref by {referrer.id}"); bot.send_message(referrer.id, f"🎉 {user.first_name or user.username or uid} joined via link!")
    except Exception as e: logger.error(f"Error proc start param for {uid}: {e}")
    updated = False
    if user.username != tg_user.username: user.username = tg_user.username; updated = True
    if user.first_name != tg_user.first_name: user.first_name = tg_user.first_name; updated = True
    if user.last_name != tg_user.last_name: user.last_name = tg_user.last_name; updated = True
    if created or updated: try: db.commit()
                           except Exception as e_comm: db.rollback(); logger.error(f"Error saving user {uid}: {e_comm}")
    
    btn_url = f"https://t.me/{bot.get_me().username}/{MINI_APP_NAME or 'app'}" 
    if not MINI_APP_NAME: logger.warning("MINI_APP_NAME not set, using fallback for button URL.")
        
    markup = types.InlineKeyboardMarkup(); web_app = types.WebAppInfo(url=btn_url)
    btn = types.InlineKeyboardButton(text="🎮 Открыть Pusik Gifts", web_app=web_app)
    markup.add(btn); bot.send_message(message.chat.id, "Добро пожаловать в Pusik Gifts! 🎁\n\nНажмите кнопку ниже!", reply_markup=markup)

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
