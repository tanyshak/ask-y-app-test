from flask import Flask
from board import pages
from dotenv import dotenv_values
import os

def create_app():
    config = dotenv_values(".env")
    pages.app.secret_key = os.getenv('SECRET_KEY')
    return pages.app
