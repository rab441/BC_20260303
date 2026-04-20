import os
from telegram import Update, InputMediaPhoto, InputMediaVideo, InputMediaDocument
from telegram.ext import Application, MessageHandler, filters, CallbackContext
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
COPY_CHAT_ID = -1002233073869

# ============ CONFIGURACIÓN DE RATE LIMITING ============
class RateLimiter:
    def __init__(self):
        # Cola global de mensajes (FIFO)
        # Aumentado a 2000 para soportar rafagas de 100 msgs x 2 destinos con margen
        self.message_queue = asyncio.Queue(maxsize=2000)

        # Tracking de mensajes por chat (para rate limit por chat)
        self.chat_timestamps = defaultdict(deque)

        # Tracking global de mensajes
        self.global_timestamps = deque()

        # Limites de envio
        # Telegram permite ~20 msgs/min por grupo/canal (1 cada 3s es el margen
        # seguro para evitar flood bans). Global: 30/seg, usamos 20 conservador.
        #
        # FIX #1: CHAT_TIME_WINDOW reemplaza la ventana fija de 1s.
        # Original:  return len(chat_times) == 0  -> exigia 0 msgs en 1s completo
        # Corregido: return len(chat_times) < CHAT_RATE_LIMIT  con ventana de 3s
        self.GLOBAL_RATE_LIMIT = 20   # mensajes por segundo globalmente
        self.CHAT_RATE_LIMIT   = 1    # mensajes por CHAT_TIME_WINDOW por chat
        self.CHAT_TIME_WINDOW  = 3.0  # ventana en segundos por chat

        # Contadores de estadisticas
        self.total_sent      = 0
        self.total_errors    = 0
        self.total_discarded = 0

        # Estado del procesador
        self.processor_running = False

    async def add_to_queue(self, message_data):
        """Anade un mensaje a la cola para procesar"""
        try:
            self.message_queue.put_nowait(message_data)
            logger.info(f"Mensaje añadido a cola. Cola actual: {self.message_queue.qsize()}")
        except asyncio.QueueFull:
            self.total_discarded += 1
            logger.error("❌ Cola de mensajes llena. Mensaje descartado.")

    async def can_send_to_chat(self, chat_id):
        """
        Verifica si se puede enviar mensaje a un chat especifico.

        FIX #1: La versión anterior retornaba True solo si el deque estaba VACIO
        (len == 0), lo que exigia que no hubiera enviado NADA en el ultimo segundo
        completo — equivalente a max 1 msg/seg/chat con gap de 1s entre cada uno.
        Para 100 msgs al mismo chat = ~100 segundos de espera minima.

        Ahora usa CHAT_TIME_WINDOW (3s) como ventana deslizante y permite
        CHAT_RATE_LIMIT mensajes dentro de esa ventana, alineado con el limite
        real de Telegram para grupos (~20 msgs/min = 1 cada 3s).
        """
        now = time.time()
        chat_times = self.chat_timestamps[chat_id]

        # Limpiar timestamps fuera de la ventana
        while chat_times and now - chat_times[0] > self.CHAT_TIME_WINDOW:
            chat_times.popleft()

        return len(chat_times) < self.CHAT_RATE_LIMIT

    async def can_send_globally(self):
        """Verifica limite global de mensajes (por segundo)"""
        now = time.time()

        # Limpiar timestamps mas antiguos de 1 segundo
        while self.global_timestamps and now - self.global_timestamps[0] > 1:
            self.global_timestamps.popleft()

        return len(self.global_timestamps) < self.GLOBAL_RATE_LIMIT

    def record_sent_message(self, chat_id):
        """Registra que se envio un mensaje"""
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
                # Obtener mensaje de la cola (esperar maximo 1 segundo)
                try:
                    message_data = await asyncio.wait_for(
                        self.message_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                chat_id = message_data['chat_id']

                # Esperar hasta que se cumplan los rate limits
                while not (await self.can_send_to_chat(chat_id) and await self.can_send_globally()):
                    await asyncio.sleep(0.05)

                # Enviar mensaje
                await self._send_message(bot, message_data)

                # Registrar envio
                self.record_sent_message(chat_id)

                # FIX #5: Pausa reducida 50ms -> 10ms.
                # El throttling real lo hacen can_send_to_chat y can_send_globally;
                # este sleep solo evita busy-loop cuando la cola esta vacia.
                await asyncio.sleep(0.01)

            except Exception as e:
                logger.error(f"❌ Error en procesador de cola: {e}")
                await asyncio.sleep(1)

    async def _send_message(self, bot, message_data):
        """
        Envia un mensaje especifico con reintentos.

        Prioriza copy_message / copy_messages cuando el dato proviene de un
        mensaje original de Telegram: mas eficiente, preserva formato, caption
        y entidades sin depender de file_ids. Los metodos send_* se mantienen
        como fallback para casos donde copy_message no aplica.
        """
        method  = message_data['method']
        chat_id = message_data['chat_id']
        kwargs  = message_data['kwargs']
        is_copy = message_data.get('is_copy', False)

        # FIX #6: Reintentos aumentados 3 -> 5 para tolerar la latencia de
        # Render free tier. Backoff exponencial: 1,2,4,8,16s = 31s total max.
        max_retries = 5

        for attempt in range(max_retries):
            try:
                if method == 'copy_message':
                    # API nativa - no expira, preserva formato
                    await bot.copy_message(chat_id=chat_id, **kwargs)

                elif method == 'copy_messages':
                    # Copia un album completo en una sola llamada
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
                    # soporte para stickers
                    await bot.send_sticker(chat_id=chat_id, **kwargs)

                elif method == 'send_animation':
                    # soporte para GIFs/animaciones
                    await bot.send_animation(chat_id=chat_id, **kwargs)

                elif method == 'send_video_note':
                    # soporte para video notas (circulos)
                    await bot.send_video_note(chat_id=chat_id, **kwargs)

                elif method == 'send_location':
                    # soporte para ubicaciones
                    await bot.send_location(chat_id=chat_id, **kwargs)

                elif method == 'send_contact':
                    # soporte para contactos
                    await bot.send_contact(chat_id=chat_id, **kwargs)

                elif method == 'forward_message':
                    # Fallback para tipos no copiables (polls, etc.)
                    await bot.forward_message(chat_id=chat_id, **kwargs)

                elif method == 'send_media_group':
                    await bot.send_media_group(chat_id=chat_id, **kwargs)

                prefix = "📄 Copia" if is_copy else "📨 Original"
                logger.info(f"✅ {prefix} [{method}] enviado a {chat_id}")
                return  # Exito: salir del loop de reintentos

            except RetryAfter as e:
                # RetryAfter NO cuenta como intento fallido: esperamos y reintentamos
                wait_time = int(e.retry_after) + 1
                logger.warning(f"⏳ Rate limit de Telegram. Esperando {wait_time}s")
                await asyncio.sleep(wait_time)

            except (TimedOut, NetworkError) as e:
                wait_time = 2 ** attempt  # Backoff exponencial
                logger.warning(f"🔄 Error de red (intento {attempt + 1}/{max_retries}). Esperando {wait_time}s: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(wait_time)
                else:
                    self.total_errors += 1
                    logger.error(f"❌ Mensaje descartado después de {max_retries} intentos: [{method}] → {chat_id}")

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
    app_url = os.environ.get('RENDER_EXTERNAL_URL', 'http://localhost:8080')

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
media_groups     = {}
processed_groups = set()


# ============ MANEJADORES DE MENSAJES ============

async def handle_message(update: Update, context: CallbackContext) -> None:
    message = update.message
    if not message:
        return

    current_chat_id = message.chat.id
    logger.info(f"📥 Mensaje recibido del chat ID: {current_chat_id}")

    media_group_id = getattr(message, 'media_group_id', None)

    # --- Helper: encolar mensaje genérico -----------------------------------
    async def queue_message(chat_id, method, is_copy=False, **kwargs):
        message_data = {
            'chat_id': chat_id,
            'method': method,
            'kwargs': kwargs,
            'is_copy': is_copy
        }
        await rate_limiter.add_to_queue(message_data)

    # --- Helper: reenviar al chat origen via copy_message -------------------
    async def queue_to_origin(from_chat_id, message_id):
        """
        Envia el mensaje de vuelta al chat donde llego usando copy_message.
        Mas eficiente que send_message/send_photo/etc.: no requiere extraer
        file_ids, preserva caption, entidades y formato original.
        """
        await queue_message(
            from_chat_id,
            'copy_message',
            is_copy=False,
            from_chat_id=from_chat_id,
            message_id=message_id
        )

    # --- Helper: copiar al COPY_CHAT_ID via copy_message --------------------
    async def queue_copy(from_chat_id, message_id):
        """
        Copia un mensaje individual al COPY_CHAT_ID usando copy_message.
        Mas confiable que re-enviar por file_id: preserva caption, entidades,
        formato y no depende de que el file_id sea valido.
        """
        if COPY_CHAT_ID != from_chat_id:
            await queue_message(
                COPY_CHAT_ID,
                'copy_message',
                is_copy=True,
                from_chat_id=from_chat_id,
                message_id=message_id
            )

    # --- Helper: encolar a ambos destinos -----------------------------------
    async def queue_both(from_chat_id, message_id):
        """Encola envio al chat origen Y copia al COPY_CHAT_ID."""
        await queue_to_origin(from_chat_id, message_id)
        await queue_copy(from_chat_id, message_id)

    # --- Mensajes de texto --------------------------------------------------
    if message.text:
        await queue_both(current_chat_id, message.message_id)
        return

    # --- Grupos de medios ---------------------------------------------------
    if media_group_id:
        group_key = f"{current_chat_id}_{media_group_id}"

        if group_key not in media_groups:
            media_groups[group_key] = {
                'media': [],
                'message_ids': [],        # IDs para copy_messages
                'chat_id': current_chat_id,
                'last_update_time': time.time(),
                'processed': False,
                # FIX #3: Flag para crear la task de procesamiento UNA sola vez.
                # Original: asyncio.create_task() se llamaba por CADA mensaje del
                # album -> N tasks con race condition al disparar simultaneamente.
                # Corregido: solo la primera llegada del grupo crea la task.
                'task_created': False
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
            group = media_groups[group_key]
            if not any(item.media == file_id for item in group['media']):
                group['media'].append(media_item)
                group['message_ids'].append(message.message_id)
                group['last_update_time'] = time.time()

                # FIX #3: Solo crear una task por grupo
                if not group['task_created']:
                    group['task_created'] = True
                    asyncio.create_task(
                        process_media_group_after_delay(group_key, context)
                    )
        return

    # --- Mensajes individuales ----------------------------------------------
    try:
        handled = False

        if message.photo:
            handled = True

        elif hasattr(message, 'video') and message.video:
            handled = True

        elif hasattr(message, 'document') and message.document:
            handled = True

        elif hasattr(message, 'audio') and message.audio:
            handled = True

        elif hasattr(message, 'voice') and message.voice:
            handled = True

        # soporte para stickers
        elif hasattr(message, 'sticker') and message.sticker:
            handled = True

        # soporte para GIFs/animaciones
        elif hasattr(message, 'animation') and message.animation:
            handled = True

        # soporte para video notas (circulos)
        elif hasattr(message, 'video_note') and message.video_note:
            handled = True

        # soporte para ubicaciones
        elif hasattr(message, 'location') and message.location:
            handled = True

        # soporte para contactos
        elif hasattr(message, 'contact') and message.contact:
            handled = True

        # Polls: forward_message a ambos destinos (copy_message no soporta polls)
        elif hasattr(message, 'poll') and message.poll:
            await queue_message(
                current_chat_id, 'forward_message', is_copy=False,
                from_chat_id=current_chat_id,
                message_id=message.message_id
            )
            if COPY_CHAT_ID != current_chat_id:
                await queue_message(
                    COPY_CHAT_ID, 'forward_message', is_copy=True,
                    from_chat_id=current_chat_id,
                    message_id=message.message_id
                )
            return  # Ya gestionamos la copia manualmente

        # Para todos los tipos manejados: origen + copia via copy_message
        if handled:
            await queue_both(current_chat_id, message.message_id)

    except Exception as e:
        logger.error(f"❌ Error procesando mensaje individual: {e}")


async def process_media_group_after_delay(group_key, context):
    """
    Espera a que lleguen todos los mensajes del album y luego lo procesa.

    FIX #3: Se llama UNA sola vez por grupo (flag task_created).
    FIX #7: Usa copy_messages (album completo en 1 llamada API) en lugar de
            send_media_group con file_ids. Preserva agrupamiento nativo y caption.
    Delay reducido 5s -> 3s para mayor velocidad de respuesta.
    """
    await asyncio.sleep(3)

    group = media_groups.get(group_key)
    if not group or group['processed']:
        return

    group['processed'] = True
    chat_id     = group['chat_id']
    message_ids = group['message_ids']

    if group_key in processed_groups:
        return

    if message_ids:
        # Enviar al chat origen
        await rate_limiter.add_to_queue({
            'chat_id': chat_id,
            'method': 'copy_messages',
            'kwargs': {
                'from_chat_id': chat_id,
                'message_ids': message_ids
            },
            'is_copy': False
        })

        # Copiar al COPY_CHAT_ID en una sola llamada
        if COPY_CHAT_ID != chat_id:
            await rate_limiter.add_to_queue({
                'chat_id': COPY_CHAT_ID,
                'method': 'copy_messages',
                'kwargs': {
                    'from_chat_id': chat_id,
                    'message_ids': message_ids
                },
                'is_copy': True
            })

    processed_groups.add(group_key)

    if group_key in media_groups:
        del media_groups[group_key]


async def cleanup_media_groups_task():
    """Limpia grupos de medios huerfanos (sin actividad por mas de 30s)"""
    while True:
        await asyncio.sleep(30)
        current_time = time.time()
        groups_to_delete = [
            k for k, v in media_groups.items()
            if current_time - v['last_update_time'] > 30
        ]

        for group_key in groups_to_delete:
            if group_key in media_groups:
                del media_groups[group_key]

        # Limitar el tamano del set de grupos procesados para evitar memory leak
        if len(processed_groups) > 500:
            processed_groups.clear()


def main() -> None:
    # Fix para Python 3.12+ / 3.14: crear el event loop explicitamente
    # antes de que Flask/threading lo interfiera
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    start_keep_alive()

    async def error_handler(update, context):
        logger.error(f"❌ Error en la actualizacion {update}: {context.error}")

    # FIX #4: Eliminado AIORateLimiter nativo de PTB.
    # Con group_max_rate=20/min actuaba DESPUES de nuestra cola personalizada,
    # anadiendo hasta 3s de latencia extra por mensaje de forma silenciosa.
    # Nuestro CHAT_TIME_WINDOW=3s ya respeta ese limite de Telegram sin overhead.
    application = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    application.add_handler(MessageHandler(filters.ALL, handle_message))
    application.add_error_handler(error_handler)

    async def post_init(app):
        # Iniciar procesador de cola personalizado
        asyncio.create_task(rate_limiter.start_processor(app.bot))
        # Iniciar limpieza de grupos de medios huerfanos
        asyncio.create_task(cleanup_media_groups_task())

    application.post_init = post_init

    logger.info("🚀 Bot iniciado con gestion de concurrencia mejorada")
    logger.info(f"📊 Limites: {rate_limiter.GLOBAL_RATE_LIMIT} msgs/seg global, "
                f"{rate_limiter.CHAT_RATE_LIMIT} msg/{rate_limiter.CHAT_TIME_WINDOW}s por chat")
    logger.info("🔒 Rate limiting por ventana deslizante activo")
    logger.info("📡 Sistema keep-alive activo")
    logger.info("🔄 Cola de mensajes configurada (maxsize=2000)")
    logger.info("📋 Tipos soportados: texto, foto, video, doc, audio, voz,")
    logger.info("   sticker, animacion, video_note, ubicacion, contacto, poll")
    logger.info("⚡ copy_message / copy_messages habilitados para origen y copia")
    logger.info(f"🎯 Destino de copia: {COPY_CHAT_ID}")

    application.run_polling()


if __name__ == '__main__':
    main()
