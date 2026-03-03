import os
from telegram import Update, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from telegram.ext import Application, MessageHandler, filters, CallbackContext, AIORateLimiter
from telegram.error import RetryAfter, TimedOut, NetworkError
import asyncio
import logging
import time
import threading
import requests
from flask import Flask
from datetime import datetime
from collections import defaultdict, deque

# ============ CONFIGURACIÓN DEL LOGGER ============
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_TOKEN")

# CHAT DE DESTINO PARA LAS COPIAS
COPY_CHAT_ID = -1001794205779

# ============ CONFIGURACIÓN DE RATE LIMITING ============
class RateLimiter:
    def __init__(self):
        # Cola global de mensajes (FIFO)
        self.message_queue = asyncio.Queue(maxsize=1000)  # Max 1000 mensajes en cola

        # Tracking de mensajes por chat (para rate limit por chat)
        self.chat_timestamps = defaultdict(deque)

        # Tracking global de mensajes
        self.global_timestamps = deque()

        # Límites (alineados con los límites reales de Telegram Bot API)
        self.GLOBAL_RATE_LIMIT = 25  # mensajes por segundo globalmente
        self.CHAT_RATE_LIMIT = 1     # mensaje por segundo por chat

        # Contadores de estadísticas
        self.total_sent = 0
        self.total_errors = 0
        self.total_discarded = 0

        # Estado del procesador
        self.processor_running = False

    async def add_to_queue(self, message_data):
        """Añade un mensaje a la cola para procesar"""
        try:
            await self.message_queue.put(message_data)
            logger.info(f"Mensaje añadido a cola. Cola actual: {self.message_queue.qsize()}")
        except asyncio.QueueFull:
            self.total_discarded += 1
            logger.error("❌ Cola de mensajes llena. Mensaje descartado.")

    async def can_send_to_chat(self, chat_id):
        """Verifica si se puede enviar mensaje a un chat específico"""
        now = time.time()
        chat_times = self.chat_timestamps[chat_id]

        # Limpiar timestamps antiguos (más de 1 segundo)
        while chat_times and now - chat_times[0] > 1:
            chat_times.popleft()

        # Verificar si podemos enviar (menos de 1 mensaje por segundo por chat)
        return len(chat_times) == 0

    async def can_send_globally(self):
        """Verifica límite global de mensajes"""
        now = time.time()

        # Limpiar timestamps antiguos (más de 1 segundo)
        while self.global_timestamps and now - self.global_timestamps[0] > 1:
            self.global_timestamps.popleft()

        # Verificar límite global
        return len(self.global_timestamps) < self.GLOBAL_RATE_LIMIT

    def record_sent_message(self, chat_id):
        """Registra que se envió un mensaje"""
        now = time.time()
        self.chat_timestamps[chat_id].append(now)
        self.global_timestamps.append(now)
        self.total_sent += 1

    async def start_processor(self, bot):
        """Inicia el procesador de cola de mensajes"""
        if self.processor_running:
            return

        self.processor_running = True
        logger.info("🔄 Procesador de cola iniciado")

        while self.processor_running:
            try:
                # Obtener mensaje de la cola (esperar máximo 1 segundo)
                try:
                    message_data = await asyncio.wait_for(
                        self.message_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                chat_id = message_data['chat_id']

                # Verificar rate limits
                while not (await self.can_send_to_chat(chat_id) and await self.can_send_globally()):
                    await asyncio.sleep(0.1)  # Esperar 100ms antes de reintentar

                # Enviar mensaje
                await self._send_message(bot, message_data)

                # Registrar envío
                self.record_sent_message(chat_id)

                # Pequeña pausa entre mensajes
                await asyncio.sleep(0.05)  # 50ms entre mensajes

            except Exception as e:
                logger.error(f"❌ Error en procesador de cola: {e}")
                await asyncio.sleep(1)

    async def _send_message(self, bot, message_data):
        """
        Envía un mensaje específico con reintentos.

        MEJORA v2: Se prioriza copy_message / copy_messages cuando el dato
        proviene de un mensaje original de Telegram, siendo más eficiente y
        confiable que re-enviar por file_id. Los métodos de envío clásicos se
        mantienen como fallback y para el chat de origen.
        """
        method  = message_data['method']
        chat_id = message_data['chat_id']
        kwargs  = message_data['kwargs']
        is_copy = message_data.get('is_copy', False)

        max_retries = 3

        for attempt in range(max_retries):
            try:
                if method == 'copy_message':
                    # ✅ NUEVO: usa la API nativa copy_message (no expira, preserva formato)
                    await bot.copy_message(chat_id=chat_id, **kwargs)

                elif method == 'copy_messages':
                    # ✅ NUEVO: copia un álbum completo en una sola llamada
                    await bot.copy_messages(chat_id=chat_id, **kwargs)

                elif method == 'send_message':
                    await bot.send_message(chat_id=chat_id, **kwargs)

                elif method == 'send_photo':
                    await bot.send_photo(chat_id=chat_id, **kwargs)

                elif method == 'send_video':
                    await bot.send_video(chat_id=chat_id, **kwargs)

                elif method == 'send_document':
                    await bot.send_document(chat_id=chat_id, **kwargs)

                elif method == 'send_audio':
                    await bot.send_audio(chat_id=chat_id, **kwargs)

                elif method == 'send_voice':
                    await bot.send_voice(chat_id=chat_id, **kwargs)

                elif method == 'send_sticker':
                    # ✅ NUEVO: soporte para stickers
                    await bot.send_sticker(chat_id=chat_id, **kwargs)

                elif method == 'send_animation':
                    # ✅ NUEVO: soporte para GIFs/animaciones
                    await bot.send_animation(chat_id=chat_id, **kwargs)

                elif method == 'send_video_note':
                    # ✅ NUEVO: soporte para video notas (círculos)
                    await bot.send_video_note(chat_id=chat_id, **kwargs)

                elif method == 'send_location':
                    # ✅ NUEVO: soporte para ubicaciones
                    await bot.send_location(chat_id=chat_id, **kwargs)

                elif method == 'send_contact':
                    # ✅ NUEVO: soporte para contactos
                    await bot.send_contact(chat_id=chat_id, **kwargs)

                elif method == 'forward_message':
                    # Fallback para tipos no copiables (polls, etc.)
                    await bot.forward_message(chat_id=chat_id, **kwargs)

                elif method == 'send_media_group':
                    await bot.send_media_group(chat_id=chat_id, **kwargs)

                prefix = "📄 Copia" if is_copy else "📨 Original"
                logger.info(f"✅ {prefix} [{method}] enviado a {chat_id}")
                break

            except RetryAfter as e:
                wait_time = int(e.retry_after) + 1
                logger.warning(f"⏳ Rate limit hit. Esperando {wait_time}s")
                await asyncio.sleep(wait_time)

            except (TimedOut, NetworkError) as e:
                wait_time = 2 ** attempt  # Backoff exponencial
                logger.warning(f"🔄 Error de red (intento {attempt + 1}). Esperando {wait_time}s: {e}")
                await asyncio.sleep(wait_time)

            except Exception as e:
                self.total_errors += 1
                logger.error(f"❌ Error enviando mensaje [{method}]: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"❌ Mensaje descartado después de {max_retries} intentos")
                break


# Instancia global del rate limiter
rate_limiter = RateLimiter()

# ============ KEEP-ALIVE SYSTEM ============
app = Flask(__name__)

@app.route('/')
@app.route('/health')
def health_check():
    queue_size = rate_limiter.message_queue.qsize()
    return {
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'bot': 'telegram_copy_bot',
        'queue_size': queue_size,
        'queue_status': 'healthy' if queue_size < 500 else 'high_load'
    }, 200

@app.route('/stats')
def stats():
    return {
        'queue_size': rate_limiter.message_queue.qsize(),
        'active_chats': len(rate_limiter.chat_timestamps),
        'processor_running': rate_limiter.processor_running,
        'total_sent': rate_limiter.total_sent,
        'total_errors': rate_limiter.total_errors,
        'total_discarded': rate_limiter.total_discarded,
    }, 200

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def keep_alive_ping():
    app_url = os.environ.get('RAILWAY_STATIC_URL', os.environ.get('RENDER_EXTERNAL_URL', 'http://localhost:8080'))

    while True:
        try:
            time.sleep(600)  # 10 minutos
            response = requests.get(f"{app_url}/health", timeout=30)
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Keep-alive ping exitoso. Cola: {data.get('queue_size', 0)}")
            else:
                logger.warning(f"⚠️ Keep-alive ping falló: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ Error en keep-alive ping: {e}")

def start_keep_alive():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("🌐 Servidor Flask iniciado")

    ping_thread = threading.Thread(target=keep_alive_ping, daemon=True)
    ping_thread.start()
    logger.info("🔄 Keep-alive ping iniciado")


# ============ GESTIÓN DE GRUPOS DE MEDIOS ============
media_groups = {}
processed_groups = set()


# ============ MANEJADORES DE MENSAJES ============

async def handle_message(update: Update, context: CallbackContext) -> None:
    message = update.message
    if not message:
        return

    current_chat_id = message.chat.id
    logger.info(f"📥 Mensaje recibido del chat ID: {current_chat_id}")

    media_group_id = getattr(message, 'media_group_id', None)

    # ─── Helper: encolar mensajes ───────────────────────────────────────────
    async def queue_message(chat_id, method, is_copy=False, **kwargs):
        message_data = {
            'chat_id': chat_id,
            'method': method,
            'kwargs': kwargs,
            'is_copy': is_copy
        }
        await rate_limiter.add_to_queue(message_data)

    # ─── Helper: encolar copia usando copy_message (API nativa) ─────────────
    async def queue_copy(from_chat_id, message_id):
        """
        Copia un mensaje individual al COPY_CHAT_ID usando copy_message.
        Más confiable que re-enviar por file_id: preserva caption, entidades,
        formato y no depende de que el file_id sea válido.
        """
        if COPY_CHAT_ID != from_chat_id:
            await queue_message(
                COPY_CHAT_ID,
                'copy_message',
                is_copy=True,
                from_chat_id=from_chat_id,
                message_id=message_id
            )

    # ─── Mensajes de texto ───────────────────────────────────────────────────
    if message.text:
        await queue_message(current_chat_id, 'send_message', text=message.text)
        await queue_copy(current_chat_id, message.message_id)
        return

    # ─── Grupos de medios ────────────────────────────────────────────────────
    if media_group_id:
        group_key = f"{current_chat_id}_{media_group_id}"

        if group_key not in media_groups:
            media_groups[group_key] = {
                'media': [],
                'message_ids': [],        # ✅ NUEVO: guardamos IDs para copy_messages
                'chat_id': current_chat_id,
                'last_update_time': time.time(),
                'processed': False
            }

        media_item = None
        caption = getattr(message, 'caption', None)

        if message.photo:
            media_item = InputMediaPhoto(media=message.photo[-1].file_id, caption=caption)
        elif hasattr(message, 'video') and message.video:
            media_item = InputMediaVideo(media=message.video.file_id, caption=caption)
        elif hasattr(message, 'document') and message.document:
            media_item = InputMediaDocument(media=message.document.file_id, caption=caption)

        if media_item:
            file_id = media_item.media
            if not any(item.media == file_id for item in media_groups[group_key]['media']):
                media_groups[group_key]['media'].append(media_item)
                media_groups[group_key]['message_ids'].append(message.message_id)
                media_groups[group_key]['last_update_time'] = time.time()

                # Programar procesamiento
                asyncio.create_task(process_media_group_after_delay(group_key, context))
        return

    # ─── Mensajes individuales ───────────────────────────────────────────────
    try:
        handled = False

        if message.photo:
            await queue_message(current_chat_id, 'send_photo',
                                photo=message.photo[-1].file_id,
                                caption=getattr(message, 'caption', None))
            handled = True

        elif hasattr(message, 'video') and message.video:
            await queue_message(current_chat_id, 'send_video',
                                video=message.video.file_id,
                                caption=getattr(message, 'caption', None))
            handled = True

        elif hasattr(message, 'document') and message.document:
            await queue_message(current_chat_id, 'send_document',
                                document=message.document.file_id,
                                caption=getattr(message, 'caption', None))
            handled = True

        elif hasattr(message, 'audio') and message.audio:
            await queue_message(current_chat_id, 'send_audio',
                                audio=message.audio.file_id,
                                caption=getattr(message, 'caption', None))
            handled = True

        elif hasattr(message, 'voice') and message.voice:
            await queue_message(current_chat_id, 'send_voice',
                                voice=message.voice.file_id,
                                caption=getattr(message, 'caption', None))
            handled = True

        # ✅ NUEVO: Stickers
        elif hasattr(message, 'sticker') and message.sticker:
            await queue_message(current_chat_id, 'send_sticker',
                                sticker=message.sticker.file_id)
            handled = True

        # ✅ NUEVO: Animaciones / GIFs
        elif hasattr(message, 'animation') and message.animation:
            await queue_message(current_chat_id, 'send_animation',
                                animation=message.animation.file_id,
                                caption=getattr(message, 'caption', None))
            handled = True

        # ✅ NUEVO: Video notas (mensajes de video circular)
        elif hasattr(message, 'video_note') and message.video_note:
            await queue_message(current_chat_id, 'send_video_note',
                                video_note=message.video_note.file_id)
            handled = True

        # ✅ NUEVO: Ubicaciones
        elif hasattr(message, 'location') and message.location:
            await queue_message(current_chat_id, 'send_location',
                                latitude=message.location.latitude,
                                longitude=message.location.longitude)
            handled = True

        # ✅ NUEVO: Contactos
        elif hasattr(message, 'contact') and message.contact:
            await queue_message(current_chat_id, 'send_contact',
                                phone_number=message.contact.phone_number,
                                first_name=message.contact.first_name,
                                last_name=getattr(message.contact, 'last_name', None))
            handled = True

        # ✅ NUEVO: Encuestas / Polls → forward (copy_message no soporta polls)
        elif hasattr(message, 'poll') and message.poll:
            await queue_message(current_chat_id, 'forward_message',
                                from_chat_id=current_chat_id,
                                message_id=message.message_id)
            if COPY_CHAT_ID != current_chat_id:
                await queue_message(COPY_CHAT_ID, 'forward_message', is_copy=True,
                                    from_chat_id=current_chat_id,
                                    message_id=message.message_id)
            return  # Ya gestionamos la copia manualmente

        # Para todos los tipos manejados, encolar copia via copy_message (API nativa)
        if handled:
            await queue_copy(current_chat_id, message.message_id)

    except Exception as e:
        logger.error(f"❌ Error procesando mensaje individual: {e}")


async def process_media_group_after_delay(group_key, context):
    await asyncio.sleep(5)

    if group_key in media_groups and not media_groups[group_key]['processed']:
        media_groups[group_key]['processed'] = True
        chat_id     = media_groups[group_key]['chat_id']
        message_ids = media_groups[group_key]['message_ids']

        if group_key in processed_groups:
            return

        media_list = media_groups[group_key]['media']

        if len(media_list) > 0:
            # Encolar grupo de medios original (send_media_group)
            message_data = {
                'chat_id': chat_id,
                'method': 'send_media_group',
                'kwargs': {'media': media_list},
                'is_copy': False
            }
            await rate_limiter.add_to_queue(message_data)

            # ✅ NUEVO: copia usando copy_messages (una sola llamada, preserva álbum)
            if COPY_CHAT_ID != chat_id and message_ids:
                copy_data = {
                    'chat_id': COPY_CHAT_ID,
                    'method': 'copy_messages',
                    'kwargs': {
                        'from_chat_id': chat_id,
                        'message_ids': message_ids
                    },
                    'is_copy': True
                }
                await rate_limiter.add_to_queue(copy_data)

            processed_groups.add(group_key)

        if group_key in media_groups:
            del media_groups[group_key]


async def cleanup_media_groups_task():
    while True:
        await asyncio.sleep(30)
        current_time = time.time()
        groups_to_delete = []

        for group_key, group_data in media_groups.items():
            if current_time - group_data['last_update_time'] > 30:
                groups_to_delete.append(group_key)

        for group_key in groups_to_delete:
            if group_key in media_groups:
                del media_groups[group_key]

        # Limitar el tamaño del set de grupos procesados
        if len(processed_groups) > 100:
            processed_groups.clear()


def main() -> None:
    start_keep_alive()

    async def error_handler(update, context):
        logger.error(f"❌ Error en la actualización {update}: {context.error}")

    # ✅ NUEVO: AIORateLimiter nativo de PTB como capa adicional de protección
    # Actúa a nivel de llamada a la API, complementando la cola personalizada.
    # Instalación requerida: pip install "python-telegram-bot[rate-limiter]"
    aio_rate_limiter = AIORateLimiter(
        overall_max_rate=25,    # 25 mensajes/seg global (igual que la cola)
        overall_time_period=1,
        group_max_rate=20,      # 20 mensajes/min por grupo
        group_time_period=60,
        max_retries=3           # Reintentos automáticos ante RetryAfter
    )

    application = (
        Application.builder()
        .token(TOKEN)
        .rate_limiter(aio_rate_limiter)   # ✅ Rate limiter nativo integrado
        .build()
    )

    application.add_handler(MessageHandler(filters.ALL, handle_message))
    application.add_error_handler(error_handler)

    async def post_init(app):
        # Iniciar procesador de cola personalizado
        asyncio.create_task(rate_limiter.start_processor(app.bot))
        # Iniciar limpieza de grupos de medios
        asyncio.create_task(cleanup_media_groups_task())

    application.post_init = post_init

    logger.info("🚀 Bot iniciado con gestión de concurrencia mejorada")
    logger.info("📊 Límites: 25 msgs/seg global, 1 msg/seg por chat")
    logger.info("🔒 AIORateLimiter nativo activado (capa adicional)")
    logger.info("📡 Sistema keep-alive activo")
    logger.info("🔄 Cola de mensajes configurada")
    logger.info("📋 Tipos soportados: texto, foto, video, doc, audio, voz,")
    logger.info("   sticker, animación, video_note, ubicación, contacto, poll")
    logger.info("⚡ copy_message / copy_messages habilitados para copias")

    application.run_polling()


if __name__ == '__main__':

    main()
