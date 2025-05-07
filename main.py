import os
import time
import asyncio
import schedule
import httpx 
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware 
from fastapi.responses import HTMLResponse # Для корневого маршрута
from sqlmodel import Field, SQLModel, create_engine, Session, select
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import random
import hmac
import hashlib
import json
from urllib.parse import unquote, parse_qs
import threading # Для запуска бота и FastAPI вместе (упрощенный вариант)

# --- Telebot ---
import telebot
from telebot import types

# --- Мониторинг Tonnel Market (Очень упрощенно и не рекомендуется для продакшена!) ---
import requests
from bs4 import BeautifulSoup

# --- Загрузка переменных окружения (для токена бота) ---
load_dotenv() 
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# Убедись, что токен задан, иначе бот не запустится
if not BOT_TOKEN:
    print("ERROR: TELEGRAM_BOT_TOKEN environment variable not set!")
    exit() # Выход, если токен не найден

# --- Константы ---
# !!! ЗАМЕНИТЬ НА СВОЙ АДРЕС КОШЕЛЬКА ДЛЯ ПОПОЛНЕНИЙ !!!
DEPOSIT_WALLET_ADDRESS = "UQAAUfBRen6rPRg_BPoheYBWIijXcI3F90s2rflQiAmzrFvb" 
# Используем путь к данным, который Render предоставляет для Persistent Disks
# Если диск примонтирован в /data, файл будет /data/gifts_app.db
# Если запускаешь локально без диска, создастся в текущей папке
DATA_DIR = os.getenv("RENDER_DISK_MOUNT_PATH", ".") # Используем переменную Render или текущую папку
DB_FILE = os.path.join(DATA_DIR, "gifts_app.db") 

# --- База данных ---
DATABASE_URL = f"sqlite+aiosqlite:///{DB_FILE}"
# connect_args важен для SQLite при использовании в разных потоках/async
engine = create_engine(DATABASE_URL, echo=False, connect_args={"check_same_thread": False}) 

# --- Модели данных SQLModel (без изменений) ---
class User(SQLModel, table=True):
    telegram_id: int = Field(default=None, primary_key=True)
    username: Optional[str] = Field(index=True, default=None)
    first_name: Optional[str] = Field(default=None)
    last_name: Optional[str] = Field(default=None)
    wallet_address: Optional[str] = Field(unique=True, index=True, default=None)
    ton_balance: float = Field(default=0.0)
    star_balance: int = Field(default=0)
    referrer_id: Optional[int] = Field(default=None, foreign_key="user.telegram_id")
    referral_code: Optional[str] = Field(unique=True, index=True, default=None)
    created_at: float = Field(default_factory=time.time)
    last_seen_at: float = Field(default_factory=time.time)
    
class InventoryItem(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.telegram_id", index=True)
    item_name: str
    item_filename: str 
    floor_price: float
    upgrade_multiplier: float = Field(default=1.0)
    received_at: float = Field(default_factory=time.time)
    
class Deposit(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="user.telegram_id", index=True)
    amount_nanotons: int 
    payload_comment: Optional[str] = Field(index=True, default=None) 
    status: str = Field(default="pending", index=True) 
    transaction_hash: Optional[str] = Field(unique=True, default=None) 
    created_at: float = Field(default_factory=time.time)
    confirmed_at: Optional[float] = Field(default=None)

# --- Pydantic модели для API (без изменений) ---
class UserDataResponse(SQLModel):
    telegram_id: int
    username: Optional[str]
    first_name: Optional[str]
    wallet_address: Optional[str]
    ton_balance: float
    star_balance: int
    inventory: List[InventoryItem]
    referral_code: Optional[str]

class OpenCaseRequest(SQLModel):
    case_id: str
    
class UpgradeItemRequest(SQLModel):
    item_inventory_id: int
    multiplier: float

class ConvertItemRequest(SQLModel):
     item_inventory_id: int
     
class WithdrawRequestPayload(SQLModel): 
    item_inventory_id: int
    
# --- Данные о кейсах (без изменений, УБЕДИСЬ ЧТО ВСЕ imageFilename ЗАПОЛНЕНЫ) ---
# Функция generateImageFilename (без изменений)
def generateImageFilename(name):
    if not name: return 'placeholder.png' 
    if name == "Durov's Cap": return "Durov's-Cap.png"; 
    if name == "Kissed Frog Happy Pepe": return "Kissed-Frog-Happy-Pepe.png"; 
    if name == "Vintage Cigar": return "Vintage-CIgar.png"; 
    return name.replace(' ', '-').replace('&', 'and') + '.png'; 

cases_data_dict: Dict[str, Dict[str, Any]] = {
    # ... (Вставь сюда ПОЛНЫЙ массив casesData из предыдущего ответа) ...
    # Пример одного кейса
     'lolpop': { 
        id: 'lolpop', name: 'Lol Pop Stash', imageFilename: generateImageFilename('Lol Pop'), priceTON: 0.5,
        prizes: [
            { name: 'Neko Helmet', imageFilename: generateImageFilename('Neko Helmet'), floorPrice: 7.5, probability: 0.01 },
            # ... остальные призы для lolpop с imageFilename ...
             { name: 'Skull Flower', imageFilename: generateImageFilename('Skull Flower'), floorPrice: 1.7, probability: 0.02 },
        ]
    },
    # ... и так далее для ВСЕХ кейсов ...
}

upgrade_chances: Dict[float, int] = { 
    1.5: 50, 2: 35, 3: 25, 5: 15, 10: 8, 20: 3
}

# --- Функции для работы с БД (без изменений) ---
def get_session():
    with Session(engine) as session:
        yield session

async def create_db_and_tables_async(): # Переименовал для ясности
    async with engine.begin() as conn:
         # await conn.run_sync(SQLModel.metadata.drop_all) # Осторожно! Удаляет все данные.
         await conn.run_sync(SQLModel.metadata.create_all)
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Lifespan startup: Creating database and tables if they don't exist...")
    await create_db_and_tables_async()
    # Запуск фоновой задачи мониторинга (нужно реализовать!)
    # deposit_monitor_task = asyncio.create_task(monitor_deposits()) 
    yield
    # deposit_monitor_task.cancel() # Остановка при завершении
    print("Lifespan shutdown.")

# --- Приложение FastAPI ---
app = FastAPI(lifespan=lifespan, title="TON Gifts API")

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # В продакшене укажи URL своего фронтенда на GitHub Pages
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Валидация Telegram InitData (без изменений) ---
def validate_init_data(init_data: str) -> Optional[Dict[str, Any]]:
    if not BOT_TOKEN:
        print("WARNING: TELEGRAM_BOT_TOKEN not set. Skipping initData validation.")
        try:
             parsed_data = {}
             data_params = parse_qs(init_data) # Используем parse_qs для правильного парсинга
             for key, value in data_params.items():
                 # parse_qs возвращает список значений, берем первое
                 parsed_data[key] = value[0] if value else None 
             
             if 'user' in parsed_data and parsed_data['user']:
                  user_data = json.loads(unquote(parsed_data['user'])) # Не забываем unquote
                  if 'id' in user_data:
                       parsed_data['user_id'] = user_data['id']
                       return parsed_data
             return None
        except Exception as e:
            print(f"Error parsing initData without validation: {e}")
            return None
            
    try:
        # Правильная валидация
        parsed_init_data = parse_qs(init_data)
        hash_from_init = parsed_init_data.get('hash', [None])[0]
        if not hash_from_init:
            return None # Нет хэша для проверки

        # Собираем строку для проверки
        data_check_arr = []
        for key in sorted(parsed_init_data.keys()):
            if key != 'hash':
                 # Значения тоже могут быть unquote'нуты
                data_check_arr.append(f"{key}={unquote(parsed_init_data[key][0])}")
        data_check_string = "\n".join(data_check_arr)
        
        secret_key = hmac.new("WebAppData".encode(), BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        if calculated_hash == hash_from_init:
             validated_data = {}
             for key, value in parsed_init_data.items():
                 validated_data[key] = unquote(value[0]) if value else None
                 
             if 'user' in validated_data and validated_data['user']:
                  user_data = json.loads(validated_data['user'])
                  if 'id' in user_data:
                       validated_data['user_id'] = user_data['id']
                       return validated_data # Возвращаем распарсенные данные, включая user
             return None # Нет 'user' поля
        else:
            print("Hash mismatch")
            return None # Хэш не совпал
    except Exception as e:
        print(f"Error validating initData: {e}")
        return None
        
# Зависимость для получения ID пользователя 
# Использует validate_init_data и создает/обновляет пользователя
async def get_current_user(request: Request, session: Session = Depends(get_session)) -> User:
    init_data_header = request.headers.get("X-Telegram-Init-Data")
    if not init_data_header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-Telegram-Init-Data header missing")
        
    validated_data = validate_init_data(init_data_header)
    
    if not validated_data or 'user' not in validated_data:
         raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid or missing initData")

    try:
        user_info = json.loads(validated_data['user'])
        user_id = int(user_info['id'])
    except (json.JSONDecodeError, KeyError, ValueError):
         raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid user data in initData")

    # Обновляем last_seen_at или создаем пользователя
    user = await session.get(User, user_id)
    current_time = time.time()
    if user:
        user.last_seen_at = current_time
        # Обновляем данные, если они изменились
        user.username = user_info.get('username', user.username)
        user.first_name = user_info.get('first_name', user.first_name)
        user.last_name = user_info.get('last_name', user.last_name)
        session.add(user) # Помечаем для обновления
    else:
        # Создаем нового пользователя
        referral_code = f"ref_{str(user_id)[-6:]}_{random.randint(100,999)}" 
        user = User(
            telegram_id=user_id, 
            username=user_info.get('username'),
            first_name=user_info.get('first_name'),
            last_name=user_info.get('last_name'),
            referral_code=referral_code,
            last_seen_at=current_time,
            created_at=current_time # Устанавливаем время создания
        )
        session.add(user)
    await session.commit()
    await session.refresh(user) # Обновляем объект user из БД
        
    return user


# --- API Эндпоинты ---
@app.get("/", response_class=HTMLResponse)
async def read_root():
    # Простой ответ для проверки, что сервер работает
    return """
    <html>
        <head><title>TON Gifts API</title></head>
        <body><h1>TON Gifts API is running!</h1></body>
    </html>
    """

@app.get("/api/user", response_model=UserDataResponse)
async def get_user_data_endpoint(current_user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    """Получает данные текущего пользователя, включая инвентарь."""
    statement = select(InventoryItem).where(InventoryItem.user_id == current_user.telegram_id)
    results = await session.exec(statement)
    inventory_items = results.all()
    
    # Убираем поле referrer_id из ответа (если не нужно на фронте)
    response_data = current_user.model_dump() # Используем model_dump для Pydantic v2 / SQLModel
    response_data['inventory'] = inventory_items
    if 'referrer_id' in response_data:
         del response_data['referrer_id'] # Не отправляем ID реферера
         
    return UserDataResponse(**response_data)


@app.post("/api/open_case")
async def open_case_endpoint(request: OpenCaseRequest, current_user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    """Открывает кейс, списывает баланс, определяет выигрыш и добавляет в инвентарь."""
    case_info = cases_data_dict.get(request.case_id)
    if not case_info:
        raise HTTPException(status_code=404, detail="Case not found")

    price_ton = case_info.get('priceTON', 0)
    price_stars = case_info.get('priceStars', 0)
    
    # Проверка баланса
    if current_user.ton_balance < price_ton:
         raise HTTPException(status_code=400, detail="Not enough TON")
    if current_user.star_balance < price_stars:
         raise HTTPException(status_code=400, detail="Not enough Stars")
         
    # Списание баланса
    current_user.ton_balance -= price_ton
    current_user.star_balance -= price_stars
    
    # Определение выигрыша
    prizes = case_info.get('prizes', [])
    if not prizes:
         # Важно! Возвращаем баланс, если нет призов
         current_user.ton_balance += price_ton
         current_user.star_balance += price_stars
         session.add(current_user) # Помечаем на сохранение
         await session.commit()
         raise HTTPException(status_code=500, detail="Case has no prizes defined")
         
    # --- Логика выбора приза ---
    winner = None
    rand = random.random()
    cumulative_probability = 0
    
    # Нормализация вероятностей 
    total_prob = sum(p.get('probability', 0) for p in prizes)
    if abs(total_prob - 1.0) > 0.001 and total_prob > 0:
        normalized_prizes = [{**p, 'probability': p.get('probability', 0) / total_prob} for p in prizes]
    elif total_prob == 0:
         normalized_prizes = [{**p, 'probability': 1.0 / len(prizes)} for p in prizes]
    else:
        normalized_prizes = prizes

    for prize in normalized_prizes:
        cumulative_probability += prize.get('probability', 0)
        if rand <= cumulative_probability:
            winner = prize
            break
    if not winner: # Fallback
         winner = normalized_prizes[-1] if normalized_prizes else None
         
    if not winner:
         current_user.ton_balance += price_ton 
         current_user.star_balance += price_stars
         session.add(current_user)
         await session.commit()
         raise HTTPException(status_code=500, detail="Could not determine winner")

    # Добавление выигрыша в инвентарь
    new_item = InventoryItem(
        user_id=current_user.telegram_id,
        item_name=winner['name'],
        item_filename=winner.get('imageFilename', generateImageFilename(winner['name'])),
        floor_price=winner['floorPrice']
    )
    session.add(new_item)
    session.add(current_user) # Добавляем юзера для сохранения баланса
    await session.commit()
    await session.refresh(new_item) 
    await session.refresh(current_user) # Обновляем user после commit
    
    return {"success": True, "won_item": new_item, "new_balance_ton": current_user.ton_balance, "new_balance_stars": current_user.star_balance}

# --- Другие эндпоинты (Upgrade, Convert, Withdraw - упрощенно) ---

@app.post("/api/upgrade_item")
async def upgrade_item_endpoint(request: UpgradeItemRequest, current_user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    item = await session.get(InventoryItem, request.item_inventory_id)
    
    if not item or item.user_id != current_user.telegram_id:
        raise HTTPException(status_code=404, detail="Item not found or doesn't belong to user")
        
    multiplier = request.multiplier
    chance = upgrade_chances.get(multiplier, 0)
    
    success = random.random() * 100 < chance
    
    if success:
        item.floor_price = round(item.floor_price * multiplier, 4) 
        item.upgrade_multiplier *= multiplier 
        session.add(item)
        await session.commit()
        await session.refresh(item)
        return {"success": True, "message": f"Success! {item.item_name} value increased.", "updated_item": item}
    else:
        item_name = item.item_name 
        await session.delete(item)
        await session.commit()
        return {"success": False, "message": f"Failed! {item_name} was destroyed."}


@app.post("/api/convert_item")
async def convert_item_endpoint(request: ConvertItemRequest, current_user: User = Depends(get_current_user), session: Session = Depends(get_session)):
    item = await session.get(InventoryItem, request.item_inventory_id)
    
    if not item or item.user_id != current_user.telegram_id:
        raise HTTPException(status_code=404, detail="Item not found or doesn't belong to user")
        
    item_name = item.item_name
    conversion_value = item.floor_price
    
    current_user.ton_balance += conversion_value
    await session.delete(item)
    session.add(current_user) 
    await session.commit()
    await session.refresh(current_user) # Обновляем пользователя после commit
    
    return {"success": True, "message": f"{item_name} converted to {conversion_value:.2f} TON.", "new_balance_ton": current_user.ton_balance}

@app.post("/api/initiate_withdrawal")
async def initiate_withdrawal_endpoint(request: WithdrawRequestPayload, current_user: User = Depends(get_current_user), session: Session = Depends(get_session)):
     item = await session.get(InventoryItem, request.item_inventory_id)
     if not item or item.user_id != current_user.telegram_id:
         raise HTTPException(status_code=404, detail="Item not found or doesn't belong to user")

     print(f"User {current_user.telegram_id} initiated withdrawal for item {item.id} ({item.item_name})")
     
     # TODO: Реализовать реальную логику вывода через Tonnel Market API / Контракты
     # Сейчас просто удаляем предмет для симуляции
     await session.delete(item)
     await session.commit()
     
     return {"success": True, "message": "Withdrawal initiated (simulation)."}
     

# --- Telegram Bot ---
bot = telebot.TeleBot(BOT_TOKEN)

@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = types.InlineKeyboardMarkup()
    # !!! ЗАМЕНИТЬ URL НА АКТУАЛЬНЫЙ URL ТВОЕГО ФРОНТЕНДА !!!
    web_app_url = "https://vasiliy-katsyka.github.io/case/" 
    web_app = types.WebAppInfo(web_app_url) 
    button = types.InlineKeyboardButton(text="🎁 Open Gift App", web_app=web_app)
    markup.add(button)
    bot.send_message(message.chat.id, "Click the button below to open the Gift Universe!", reply_markup=markup)

# --- Запуск FastAPI и Бота ---

# Функция для запуска FastAPI
def run_fastapi():
    import uvicorn
    # Запускаем на порту 8000 (или другом, если он занят)
    # Render сам пробросит порт 10000 на 80/443
    # При локальном запуске обращаемся к http://127.0.0.1:8000
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)), log_level="info") 

# Функция для запуска бота
def run_bot():
    print("Starting Telegram bot polling...")
    bot.polling(non_stop=True, interval=1) # interval для уменьшения нагрузки

if __name__ == "__main__":
    # Запускаем FastAPI в отдельном потоке (упрощенный вариант для Render)
    # В продакшене лучше использовать Gunicorn + Uvicorn workers
    fastapi_thread = threading.Thread(target=run_fastapi, daemon=True)
    fastapi_thread.start()
    
    # Запускаем бота в основном потоке
    run_bot()
