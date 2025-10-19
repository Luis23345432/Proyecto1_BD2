from __future__ import annotations

import os
from typing import List
from fastapi import APIRouter, HTTPException

from .schemas import UserCreate, User
from engine import DatabaseEngine

router = APIRouter(prefix="/users", tags=["users"])


def _users_root(engine: DatabaseEngine) -> str:
    return os.path.join(engine.root_dir, "data", "users")


@router.post("", response_model=User, status_code=201)
def create_user(payload: UserCreate) -> User:
    # Create user directory structure
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    user_dir = os.path.join(_users_root(engine), payload.user_id)
    os.makedirs(os.path.join(user_dir, "databases"), exist_ok=True)
    return User(user_id=payload.user_id)


@router.get("", response_model=List[User])
def list_users() -> List[User]:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    root = _users_root(engine)
    if not os.path.exists(root):
        return []
    out = []
    for name in os.listdir(root):
        if os.path.isdir(os.path.join(root, name)):
            out.append(User(user_id=name))
    return out


@router.get("/{user_id}", response_model=User)
def get_user(user_id: str) -> User:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    root = _users_root(engine)
    if not os.path.isdir(os.path.join(root, user_id)):
        raise HTTPException(status_code=404, detail="User not found")
    return User(user_id=user_id)
