import asyncio
import dataclasses
import functools
import logging
import random
from typing import Optional

import firebase_admin
from firebase_admin import storage
from firebase_functions import https_fn 
from firebase_functions.params import SecretParam, StringParam
from telegram import Update, Message
from telegram.constants import ChatAction
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Config and secrets
BOT_TOKEN = SecretParam('BOT_TOKEN')
ENDPOINT = StringParam('ENDPOINT')
REGION = StringParam('REGION', default='asia-southeast1')

# Cloud storage helpers
firebase_admin.initialize_app()
bucket = storage.bucket()

def random_blob(prefix: Optional[str] = None, delimiter: Optional[str] = None):
    return safe_pipe(
        bucket.list_blobs(prefix=prefix, delimiter=delimiter, fields='items(name)'),
        lambda blobs: list({blob.name for blob in blobs if blob.name != prefix}),
        random.choice,
        bucket.blob,
    )

# Telebot initialization and handlers
def _send_chat_action(chat_action: ChatAction):
    def decorator(handler):
        @functools.wraps(handler)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE): 
            await update.message.reply_chat_action(chat_action)
            await handler(update=update, context=context)
        return wrapper
    return decorator

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_gif(update, context)
    await greet(update, context)

@_send_chat_action(ChatAction.TYPING)
async def greet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_markdown('honk honk (/help to call for help ❤️ 🔪)')
 
@_send_chat_action(ChatAction.TYPING)
async def help_(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_markdown(
        "It's a lovely morning in the village, and you are a horrible goose."
        '\n'
        '\n' '*Commands*'
        '\n' '/start to say hello'
        '\n' '/help shows this message'
        '\n' '/honk to honk..?'
        '\n'
        '\n' '*Talk to a goose*'
        '\n' '(In groups, only responds to replies to reduce spam)'
        '\n' 'press x to honk'
        '\n' 'press X to HONK'
    )

@_send_chat_action(ChatAction.RECORD_VOICE)
async def honk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_pipe(
        random_blob(prefix='audio/'),
        lambda blob: blob.download_as_bytes(),
        lambda file: update.message.reply_voice(file, duration=1),
    )

@_send_chat_action(ChatAction.UPLOAD_PHOTO)
async def send_gif(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await safe_pipe(
        random_blob(prefix='media/hello/', delimiter='/'),
        lambda blob: blob.download_as_bytes(),
        lambda file: update.message.reply_document(file, filename='hello.gif'),
    )

@_send_chat_action(ChatAction.TYPING)
async def press_x_to_honk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    words = update.message.text.split()
    all_x = (word for word in words if all(c=='x' for c in word.lower()))
    honks = (xs.replace('x', 'honk').replace('X', 'HONK') for xs in all_x)
    if (reply := ' '.join(honks)):
        await update.message.reply_text(reply)

@_send_chat_action(ChatAction.TYPING)
async def oops(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text('Sad honks (something went wrong 💔🔪)')

# Cloud functions pipeline
@dataclasses.dataclass
class AppBuilder:
    def _build_application(self, token: str) -> Application:
        return (
            Application.builder()
            .token(token)
            .updater(None)
            .job_queue(None)    
            .build()
        )

    def _register_handlers(self, application: Application) -> Application:
        application.add_handlers([
            CommandHandler('start', start),
            CommandHandler('help', help_),
            CommandHandler('honk', honk),
            MessageHandler(filters.VOICE, honk),
            MessageHandler(filters.PHOTO | filters.VIDEO, send_gif),
            MessageHandler(filters.TEXT & ~filters.COMMAND, press_x_to_honk),
        ])
        application.add_error_handler(oops)
        return application

@dataclasses.dataclass
class RequestHandler:
    """Wrapper around a telegram application to help process raw HTTPS requests"""
    telegram_app: Application
    
    def _extract_json_from_request(self, request: https_fn.Request) -> dict:
        return request.get_json(force=True)

    def _convert_json_to_update(self, data: dict) -> Optional[Update]:
        return Update.de_json(data=data, bot=self.telegram_app.bot)

    def _handle_update_with_app(self, update: Update) -> https_fn.Response:
        async def task() -> None:
            await self.telegram_app.initialize()
            await self.telegram_app.process_update(update)
            await self.telegram_app.shutdown()
        asyncio.run(task())
        return https_fn.Response("ok")

    def _set_bot_webhook(self, url: str) -> https_fn.Response:
        task = self.telegram_app.bot.set_webhook(url=url)
        asyncio.run(task)
        return https_fn.Response('Set webhook')

@https_fn.on_request(region=REGION, secrets=[BOT_TOKEN])
def handle_request(request: https_fn.Request) -> https_fn.Response:
    """Entry point for serverless telebot request-handling pipeline"""
    builder = AppBuilder()
    app = safe_pipe(
        BOT_TOKEN.value,
        builder._build_application,
        builder._register_handlers,
    )
    
    handler = RequestHandler(telegram_app=app)
    if request.method == "GET":
        return safe_pipe(
            ENDPOINT.value,
            handler._set_bot_webhook,
        )
    if request.method == "POST":
        return safe_pipe(
            request,
            handler._extract_json_from_request,
            handler._convert_json_to_update,
            handler._handle_update_with_app,
        ) 
    raise https_fn.HttpsError(
        code=https_fn.FunctionsErrorCode('unimplemented'), 
        message=f'{request.method} not supported',
    )

# Control flow and error handling utils
def safe_pipe(value, *steps):
    """Passes a values through a pipeline of functions wrapped with error handling"""
    steps = map(add_logging, steps)
    steps = map(abort_on_error, steps)
    return functools.reduce(lambda x, f: f(x), steps, value)

def add_logging(func):
    """logs success and failure cases of function calls"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logging.debug(f'[ATTEMPT] {func.__qualname__}')
            result = func(*args, **kwargs)
            logging.debug(f'[SUCCESS] {func.__qualname__}: {args=} {kwargs=} {result=}')
            return result
        except Exception as error:
            logging.exception(f'[FAILURE] {func.__qualname__}: {args=} {kwargs=} {error=}')
            raise error
    return wrapper

def abort_on_error(func):
    """Raises a 400 error if the function fails"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as error:
            raise https_fn.HttpsError(
                code=https_fn.FunctionsErrorCode('aborted'),
                message=f'error in {func.__qualname__}: {error}',
            ) from error
    return wrapper

