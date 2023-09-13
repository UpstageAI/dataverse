
"""
UFL (Upstage Format for LLM)
"""

import uuid

def get_uuidv1():
    return uuid.uuid1().hex

def get_uuidv4():
    return uuid.uuid4().hex