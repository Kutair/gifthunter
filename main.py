import os
import time
import asyncio
import schedule
import httpx # Для асинхронных HTTP-запросов (мониторинг TON)
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware # Если фронтенд будет на другом домене
from sqlmodel import Field, SQLModel, create_engine, Session, select
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import random
import hmac
import hashlib
import json
from urllib.parse import unquote

# --- Мониторинг Tonnel Market (Очень упрощенно и не рекомендуется для продакшена!) ---
# Требует установки: pip install requests beautifulsoup4
import requests
from bs4 import BeautifulSoup

# --- Загрузка переменных окружения (для токена бота) ---
load_dotenv() 
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# --- Константы ---
# !!! ЗАМЕНИТЬ НА СВОЙ АДРЕС КОШЕЛЬКА ДЛЯ ПОПОЛНЕНИЙ !!!
DEPOSIT_WALLET_ADDRESS = "UQAAUfBRen6rPRg_BPoheYBWIijXcI3F90s2rflQiAmzrFvb" 
DB_FILE = "gifts_app.db" # Файл БД SQLite будет в той же папке
# --- База данных ---
DATABASE_URL = f"sqlite+aiosqlite:///{DB_FILE}"
engine = create_engine(DATABASE_URL, echo=False) # echo=True для отладки SQL запросов

# --- Модели данных SQLModel ---
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
    amount_nanotons: int # Храним в интах для точности
    payload_comment: Optional[str] = Field(index=True, default=null) # Важно для сопоставления!
    status: str = Field(default="pending", index=True) # pending, confirmed, failed
    transaction_hash: Optional[str] = Field(unique=True, default=null) # Хэш транзакции TON
    created_at: float = Field(default_factory=time.time)
    confirmed_at: Optional[float] = Field(default=None)

# --- Pydantic модели для API (запросы/ответы) ---
class UserDataResponse(SQLModel):
    telegram_id: int
    username: Optional[str]
    first_name: Optional[str]
    wallet_address: Optional[str]
    ton_balance: float
    star_balance: int
    inventory: List[InventoryItem]
    referral_code: Optional[str]
    # ... другие поля по необходимости

class OpenCaseRequest(SQLModel):
    case_id: str
    # user_id будет получен из initData
    
class UpgradeItemRequest(SQLModel):
    item_inventory_id: int
    multiplier: float

class ConvertItemRequest(SQLModel):
     item_inventory_id: int
     
class WithdrawRequestPayload(SQLModel): # Для запроса на вывод (со стороны фронта)
    item_inventory_id: int
    
# --- Данные о кейсах (перенесены с фронтенда) ---
# Функция generateImageFilename нужна здесь или передавать имена файлов напрямую
def generateImageFilename(name):
    if not name: return 'placeholder.png' 
    if name == "Durov's Cap": return "Durov's-Cap.png"; 
    if name == "Kissed Frog Happy Pepe": return "Kissed-Frog-Happy-Pepe.png"; 
    if name == "Vintage Cigar": return "Vintage-CIgar.png"; 
    return name.replace(/\s+/g, '-').replace(/&/g, 'and') + '.png'; 
    
# !!! ВАЖНО: Убедись, что все imageFilename здесь указаны правильно !!!
cases_data_dict: Dict[str, Dict[str, Any]] = {
    # Преобразуем массив в словарь для удобного доступа по ID
    case['id']: case for case in [
        # ... (Вставь сюда ПОЛНЫЙ массив casesData из предыдущего ответа, 
        #      убедившись, что imageFilename задан для ВСЕХ кейсов и ВСЕХ призов) ...
         { 
            id: 'lolpop', name: 'Lol Pop Stash', imageFilename: generateImageFilename('Lol Pop'), priceTON: 0.5,
            prizes: [
                { name: 'Neko Helmet', imageFilename: generateImageFilename('Neko Helmet'), floorPrice: 7.5, probability: 0.01 },
                # ... остальные призы для lolpop с imageFilename ...
            ]
        },
        # ... и так далее для ВСЕХ кейсов ...
    ] if case # Защита от пустых элементов если копипаст не удался
}

upgrade_chances: Dict[float, int] = { 
    1.5: 50, 2: 35, 3: 25, 5: 15, 10: 8, 20: 3
}

# --- Функции для работы с БД ---
def get_session():
    with Session(engine) as session:
        yield session

async def create_db_and_tables():
    # Используем асинхронную версию для aiosqlite
    async with engine.connect() as conn:
         # await conn.run_sync(SQLModel.metadata.drop_all) # Раскомментируй для сброса таблиц при разработке
         await conn.run_sync(SQLModel.metadata.create_all)
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Действия при старте приложения
    print("Creating database and tables...")
    await create_db_and_tables()
    # Запуск фоновой задачи мониторинга (примерно)
    # asyncio.create_task(monitor_deposits()) 
    yield
    # Действия при остановке приложения (если нужны)
    print("Application shutdown.")

# --- Приложение FastAPI ---
app = FastAPI(lifespan=lifespan)

# --- CORS (если фронтенд на другом домене/порту) ---
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"], # Укажи конкретный домен фронтенда в продакшене
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# --- Валидация Telegram InitData ---
# Очень важно для безопасности!
def validate_init_data(init_data: str) -> Optional[Dict[str, Any]]:
    if not BOT_TOKEN:
        print("WARNING: TELEGRAM_BOT_TOKEN not set. Skipping initData validation.")
        # В режиме разработки можно парсить без валидации, НО НЕ В ПРОДАКШЕНЕ
        try:
             # Простой парсинг для отладки (НЕБЕЗОПАСНО)
             parsed_data = {}
             for item in init_data.split('&'):
                 key, value = item.split('=', 1)
                 parsed_data[key] = unquote(value)
             if 'user' in parsed_data:
                  user_data = json.loads(parsed_data['user'])
                  # Добавляем ID пользователя в основной словарь для удобства
                  if 'id' in user_data:
                       parsed_data['user_id'] = user_data['id']
                  return parsed_data
             return None
        except Exception:
            return None
            
    try:
        # Правильная валидация
        init_data_pairs = sorted([chunk.split('=', 1) for chunk in init_data.split('&')], key=lambda x: x[0])
        data_check_string = "\n".join([f"{key}={value}" for key, value in init_data_pairs if key != 'hash'])
        secret_key = hmac.new("WebAppData".encode(), BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        provided_hash = dict(init_data_pairs).get('hash')

        if calculated_hash == provided_hash:
             parsed_data = {}
             for key, value in init_data_pairs:
                 parsed_data[key] = unquote(value)
             if 'user' in parsed_data:
                  user_data = json.loads(parsed_data['user'])
                  if 'id' in user_data:
                       parsed_data['user_id'] = user_data['id']
                  return parsed_data # Возвращаем распарсенные данные, включая user
             return None # Нет 'user' поля
        else:
            return None # Хэш не совпал
    except Exception as e:
        print(f"Error validating initData: {e}")
        return None

# Зависимость для получения ID пользователя из заголовка или initData
async def get_current_user_id(request: Request) -> int:
    init_data_header = request.headers.get("X-Telegram-Init-Data")
    if not init_data_header:
        raise HTTPException(status_code=401, detail="X-Telegram-Init-Data header missing")
        
    validated_data = validate_init_data(init_data_header)
    
    if not validated_data or 'user_id' not in validated_data:
         raise HTTPException(status_code=403, detail="Invalid or missing initData")

    user_id = int(validated_data['user_id'])
    
    # Обновляем last_seen_at или создаем пользователя
    async with Session(engine) as session:
        user = await session.get(User, user_id)
        current_time = time.time()
        if user:
            user.last_seen_at = current_time
            # Опционально: Обновить username/first_name/last_name если они изменились
            user_info = json.loads(validated_data.get('user', '{}'))
            user.username = user_info.get('username', user.username)
            user.first_name = user_info.get('first_name', user.first_name)
            user.last_name = user_info.get('last_name', user.last_name)
        else:
            # Создаем нового пользователя
            user_info = json.loads(validated_data.get('user', '{}'))
            referral_code = f"ref_{str(user_id)[-6:]}_{random.randint(100,999)}" # Генерируем код
            user = User(
                telegram_id=user_id, 
                username=user_info.get('username'),
                first_name=user_info.get('first_name'),
                last_name=user_info.get('last_name'),
                referral_code=referral_code,
                last_seen_at=current_time
            )
            session.add(user)
        await session.commit()
        
    return user_id


# --- API Эндпоинты ---
@app.get("/api/user", response_model=UserDataResponse)
async def get_user_data(user_id: int = Depends(get_current_user_id), session: Session = Depends(get_session)):
    """Получает данные пользователя, включая инвентарь."""
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found") # Должен создаваться в get_current_user_id
        
    statement = select(InventoryItem).where(InventoryItem.user_id == user_id)
    results = await session.exec(statement)
    inventory_items = results.all()
    
    return UserDataResponse(
        telegram_id=user.telegram_id,
        username=user.username,
        first_name=user.first_name,
        wallet_address=user.wallet_address,
        ton_balance=user.ton_balance,
        star_balance=user.star_balance,
        inventory=inventory_items,
        referral_code=user.referral_code
    )

@app.post("/api/open_case")
async def open_case(request: OpenCaseRequest, user_id: int = Depends(get_current_user_id), session: Session = Depends(get_session)):
    """Открывает кейс, списывает баланс, определяет выигрыш и добавляет в инвентарь."""
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    case_info = cases_data_dict.get(request.case_id)
    if not case_info:
        raise HTTPException(status_code=404, detail="Case not found")

    price_ton = case_info.get('priceTON', 0)
    price_stars = case_info.get('priceStars', 0)
    
    # Проверка баланса
    if user.ton_balance < price_ton:
         raise HTTPException(status_code=400, detail="Not enough TON")
    if user.star_balance < price_stars:
         raise HTTPException(status_code=400, detail="Not enough Stars")
         
    # Списание баланса
    user.ton_balance -= price_ton
    user.star_balance -= price_stars
    
    # Определение выигрыша
    prizes = case_info.get('prizes', [])
    if not prizes:
         raise HTTPException(status_code=500, detail="Case has no prizes defined")
         
    # --- Логика выбора приза (как на фронте) ---
    winner = None
    rand = random.random()
    cumulative_probability = 0
    
    # Нормализация вероятностей (на всякий случай)
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
         # Если победителя нет (очень странно, но может случиться при ошибках)
         # Не списываем баланс и возвращаем ошибку
         user.ton_balance += price_ton # Вернуть списанное
         user.star_balance += price_stars
         await session.commit()
         raise HTTPException(status_code=500, detail="Could not determine winner")

    # Добавление выигрыша в инвентарь
    new_item = InventoryItem(
        user_id=user_id,
        item_name=winner['name'],
        item_filename=winner.get('imageFilename', generateImageFilename(winner['name'])),
        floor_price=winner['floorPrice']
    )
    session.add(new_item)
    await session.commit()
    await session.refresh(new_item) # Получаем ID созданного предмета
    
    # Можно еще записать историю открытия кейса в отдельную таблицу
    
    return {"success": True, "won_item": new_item, "new_balance_ton": user.ton_balance, "new_balance_stars": user.star_balance}

# --- Другие эндпоинты (Upgrade, Convert, Withdraw - упрощенно) ---

@app.post("/api/upgrade_item")
async def upgrade_item(request: UpgradeItemRequest, user_id: int = Depends(get_current_user_id), session: Session = Depends(get_session)):
    user = await session.get(User, user_id)
    item = await session.get(InventoryItem, request.item_inventory_id)
    
    if not user or not item or item.user_id != user_id:
        raise HTTPException(status_code=404, detail="Item or user not found")
        
    multiplier = request.multiplier
    chance = upgrade_chances.get(multiplier, 0)
    
    success = random.random() * 100 < chance
    
    if success:
        item.floor_price = round(item.floor_price * multiplier, 4) # Округляем
        item.upgrade_multiplier *= multiplier # Можно отслеживать общий множитель
        session.add(item)
        await session.commit()
        await session.refresh(item)
        return {"success": True, "message": f"Success! {item.item_name} value increased.", "updated_item": item}
    else:
        item_name = item.item_name # Сохраняем имя перед удалением
        await session.delete(item)
        await session.commit()
        # Здесь можно добавить логику отправки уведомления или записи "сгоревшего" предмета
        return {"success": False, "message": f"Failed! {item_name} was destroyed."}


@app.post("/api/convert_item")
async def convert_item(request: ConvertItemRequest, user_id: int = Depends(get_current_user_id), session: Session = Depends(get_session)):
    user = await session.get(User, user_id)
    item = await session.get(InventoryItem, request.item_inventory_id)
    
    if not user or not item or item.user_id != user_id:
        raise HTTPException(status_code=404, detail="Item or user not found")
        
    item_name = item.item_name
    conversion_value = item.floor_price
    
    user.ton_balance += conversion_value
    await session.delete(item)
    session.add(user) # Добавляем пользователя для сохранения измененного баланса
    await session.commit()
    
    return {"success": True, "message": f"{item_name} converted to {conversion_value:.2f} TON.", "new_balance_ton": user.ton_balance}

# --- Эндпоинт для вывода (только инициирует процесс на бэке) ---
@app.post("/api/initiate_withdrawal")
async def initiate_withdrawal(request: WithdrawRequestPayload, user_id: int = Depends(get_current_user_id), session: Session = Depends(get_session)):
     # TODO: Реализовать реальную логику взаимодействия с Tonnel Market API / Смарт-контрактами
     # Этот эндпоинт пока просто симулирует начало процесса
     item = await session.get(InventoryItem, request.item_inventory_id)
     if not item or item.user_id != user_id:
         raise HTTPException(status_code=404, detail="Item not found or doesn't belong to user")

     print(f"User {user_id} initiated withdrawal for item {item.id} ({item.item_name})")
     
     # Здесь должна быть логика:
     # 1. Проверка, взаимодействовал ли пользователь с @giftrelayer (сложно без доп. данных)
     # 2. Взаимодействие с Tonnel Market API (если оно есть) или напрямую с контрактом
     # 3. Удаление предмета из инвентаря ПОСЛЕ успешной отправки
     
     # Симуляция: удаляем предмет сразу (НЕПРАВИЛЬНО для продакшена!)
     await session.delete(item)
     await session.commit()
     
     return {"success": True, "message": "Withdrawal initiated (simulation)."}
     
     
# --- Фоновая задача для мониторинга депозитов (ОЧЕНЬ упрощенный пример) ---
# async def check_ton_transactions():
#      # В реальном приложении используй TON Center API v3, toncenter.com API, dton.io API или свой индексатор
#      # Пример с httpx (нужно знать API эндпоинт для получения транзакций)
#      # api_url = f"https://toncenter.com/api/v2/getTransactions?address={DEPOSIT_WALLET_ADDRESS}&limit=10&archival=false"
#      # headers = {"accept": "application/json"} 
#      # try:
#      #     async with httpx.AsyncClient() as client:
#      #         response = await client.get(api_url, headers=headers)
#      #         response.raise_for_status() # Вызовет исключение при ошибке HTTP
#      #         transactions = response.json().get('result', [])
#      #         
#      #         async with Session(engine) as session:
#      #             for tx in transactions:
#      #                  # Парсим транзакцию
#      #                  tx_hash = tx.get('transaction_id', {}).get('hash')
#      #                  in_msg = tx.get('in_msg', {})
#      #                  source = in_msg.get('source')
#      #                  destination = in_msg.get('destination')
#      #                  value_nanotons = int(in_msg.get('value', 0))
#      #                  msg_data = in_msg.get('msg_data', {}).get('text') # Или парсить body для payload
#      #                  
#      #                  # Проверяем, что транзакция на наш адрес и не обработана
#      #                  if destination == DEPOSIT_WALLET_ADDRESS and tx_hash:
#      #                       # Ищем соответствующий 'pending' депозит (например, по payload/комментарию)
#      #                       # stmt = select(Deposit).where(Deposit.status == 'pending', Deposit.payload_comment == извлеченный_комментарий)
#      #                       # pending_deposit = await session.exec(stmt).first()
#      #                       
#      #                       # Если нашли и сумма совпадает:
#      #                       # if pending_deposit and pending_deposit.amount_nanotons == value_nanotons:
#      #                       #      user = await session.get(User, pending_deposit.user_id)
#      #                       #      if user:
#      #                       #          user.ton_balance += value_nanotons / 1_000_000_000.0
#      #                       #          pending_deposit.status = 'confirmed'
#      #                       #          pending_deposit.transaction_hash = tx_hash
#      #                       #          pending_deposit.confirmed_at = time.time()
#      #                       #          session.add(user)
#      #                       #          session.add(pending_deposit)
#      #                       #          await session.commit()
#      #                       #          print(f"Confirmed deposit for user {user.telegram_id}")
#      #                       #      else: # Пользователя нет, но депозит есть? Ошибка
#      #                       #          pending_deposit.status = 'failed' # Или 'orphan'
#      #                       #          await session.commit()
#      #                       
#      # except httpx.HTTPStatusError as e:
#      #     print(f"HTTP error checking transactions: {e.response.status_code} - {e.response.text}")
#      # except Exception as e:
#      #     print(f"Error checking transactions: {e}")
#      pass # Убрать pass при реализации

# async def monitor_deposits():
#      schedule.every(1).minutes.do(lambda: asyncio.create_task(check_ton_transactions()))
#      while True:
#           schedule.run_pending()
#           await asyncio.sleep(30) # Проверять каждые 30 секунд
