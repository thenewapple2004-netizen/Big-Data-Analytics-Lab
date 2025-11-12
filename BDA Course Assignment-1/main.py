import os
import json
import posixpath
from uuid import uuid4
from hdfs import InsecureClient
from dotenv import load_dotenv

load_dotenv()

HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT = int(os.getenv("HDFS_PORT", "9871"))
HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/users")
HDFS_USERS_FILE = posixpath.join(HDFS_BASE_DIR, "users.json")

def connect_to_hdfs():
    url = f"http://{HDFS_HOST}:{HDFS_PORT}"
    print(f"\nAttempting to connect to HDFS at {url} ...")
    try:
        client = InsecureClient(url, user=HDFS_USER)
        client.makedirs(HDFS_BASE_DIR)
        if not client.status(HDFS_USERS_FILE, strict=False):
            with client.write(HDFS_USERS_FILE, overwrite=True, encoding="utf-8") as w:
                json.dump([], w)
        print("Connected to HDFS successfully!")
        return client
    except Exception as e:
        print(f"ERROR: HDFS connection failed:\n{e}")
        print("\nTroubleshooting:")
        print("1. docker compose up -d")
        print("2. Open http://localhost:9871")
        print("3. docker ps / docker logs HDFS")
        exit(1)

def _load_all_users(client):
    try:
        with client.read(HDFS_USERS_FILE, encoding="utf-8") as r:
            return json.load(r)
    except FileNotFoundError:
        return []

def _save_all_users(client, users):
    with client.write(HDFS_USERS_FILE, overwrite=True, encoding="utf-8") as w:
        json.dump(users, w, ensure_ascii=False, indent=2)

def create_user(client, name, email):
    users = _load_all_users(client)
    if any(u.get("name") == name for u in users):
        print("User with that name already exists.")
        return
    user = {"id": str(uuid4()), "name": name, "email": email}
    users.append(user)
    _save_all_users(client, users)
    print(f"Created: {user['id']}")

def read_users(client):
    users = _load_all_users(client)
    print("\nAll Users:")
    if not users:
        print("(no users found)")
        return
    for u in users:
        print(f"ID: {u['id']} | Name: {u['name']} | Email: {u['email']}")

def update_user(client, name, new_email):
    users = _load_all_users(client)
    updated = 0
    for u in users:
        if u.get("name") == name:
            u["email"] = new_email
            updated += 1
    if updated:
        _save_all_users(client, users)
        print(f"Updated {updated} user(s).")
    else:
        print("No user found.")

def delete_user(client, name):
    users = _load_all_users(client)
    new_users = [u for u in users if u.get("name") != name]
    if len(new_users) != len(users):
        _save_all_users(client, new_users)
        print(f"Deleted '{name}'.")
    else:
        print("No user found.")

def search_user(client, name):
    users = _load_all_users(client)
    for u in users:
        if u.get("name") == name:
            print("\nFound:")
            print(f"ID: {u['id']}\nName: {u['name']}\nEmail: {u['email']}")
            return
    print("No user found.")

def display_menu():
    print("\n" + "=" * 50)
    print("HDFS CRUD OPERATIONS MENU")
    print("=" * 50)
    print("1. Create User")
    print("2. Read All Users")
    print("3. Update User Email")
    print("4. Delete User")
    print("5. Search User by Name")
    print("6. Exit")
    print("=" * 50)

def get_user_input(prompt):
    try:
        return input(prompt).strip()
    except KeyboardInterrupt:
        print("\nExiting...")
        exit()

def main():
    print("\nStarting HDFS CRUD Operations...\n")
    client = connect_to_hdfs()
    while True:
        display_menu()
        choice = get_user_input("\nEnter your choice (1-6): ")
        if choice == "1":
            name = get_user_input("Enter user name: ")
            email = get_user_input("Enter user email: ")
            if name and email:
                create_user(client, name, email)
            else:
                print("Name and email required.")
        elif choice == "2":
            read_users(client)
        elif choice == "3":
            name = get_user_input("User name to update: ")
            new_email = get_user_input("New email: ")
            if name and new_email:
                update_user(client, name, new_email)
            else:
                print("Name and new email required.")
        elif choice == "4":
            name = get_user_input("User name to delete: ")
            confirm = get_user_input(f"Delete '{name}'? (y/N): ")
            if confirm.lower() in ["y", "yes"]:
                delete_user(client, name)
            else:
                print("Cancelled.")
        elif choice == "5":
            name = get_user_input("User name to search: ")
            search_user(client, name)
        elif choice == "6":
            print("\nGoodbye!")
            break
        else:
            print("Enter a number 1–6.")
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()
