from telethon import TelegramClient, events, Button
import re
import time
from pyrule34 import AsyncRule34
import asyncio
import aiohttp
from typing import List, Tuple, Optional
import shutil
import tempfile
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Access the environment variables
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Initialize the client
client = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# Global settings
user_preferences = {}  # Dictionary to store user preferences
NSFW_MODE = ["off", "on"]
CONTENT_MODE = ["image", "video", "all"]
MAX_GROUP_SIZE = 10 * 1024 * 1024  # 10MB in bytes
MAX_SINGLE_FILE = 50 * 1024 * 1024  # 50MB in bytes

# Create a temporary directory for downloaded media
TEMP_DIR = tempfile.mkdtemp(prefix='telegram_media_')
print(f"Created temporary directory: {TEMP_DIR}")

# Create a global aiohttp session
session = None

# Semaphore for limiting concurrent operations
MAX_CONCURRENT_OPERATIONS = 3
semaphore = None


async def get_session():
    """Get or create an aiohttp session"""
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession()
    return session


async def get_semaphore():
    """Get or create a semaphore for concurrent operations"""
    global semaphore
    if semaphore is None:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_OPERATIONS)
    return semaphore


@client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    """Handle the /start command"""
    buttons = [
        [Button.url("R34 xxx", "https://r34xxx.com/index.php")],
        [Button.url("Open Rule34 Vault", "https://rule34vault.com")]
    ]

    image_path = os.path.join(os.getcwd(), "downloaded_media_new_nsfw", "image.jpeg")
    try:
        await event.respond(file=image_path)
        print("Image sent successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

    await event.respond(
        "Welcome! I'm a r34 bot. You can enter tags for the output of r34 images\n"
        "Tags should be entered like in example:\nbig breasts, big ass, -tentacle pid 10 limit 10"
        "tags with '-' sign will be excluded from the output\n"
        "pid - where you want to start uploading from\n"
        "limit - how many images you want to upload\n"
        "\n\nAvailable commands:\n"
        "/change_nsfw_mode to change NSFW mode\n"
        "/change_content_mode to change content mode\n",
        buttons=buttons
    )


@client.on(events.NewMessage(pattern='/change_nsfw_mode'))
async def change_nsfw_mode_handler(event):
    """Handle the /change_nsfw_mode command"""
    user_id = event.sender_id
    if user_id not in user_preferences:
        user_preferences[user_id] = {'nsfw_mode': True, 'content_mode': 0}
    user_preferences[user_id]['nsfw_mode'] = not user_preferences[user_id]['nsfw_mode']
    await event.respond(f"NSFW mode changed to {NSFW_MODE[user_preferences[user_id]['nsfw_mode']]}")


@client.on(events.NewMessage(pattern='/change_content_mode'))
async def change_content_mode_handler(event):
    """Handle the /change_content_mode command"""
    user_id = event.sender_id
    if user_id not in user_preferences:
        user_preferences[user_id] = {'nsfw_mode': True, 'content_mode': 0}
    user_preferences[user_id]['content_mode'] = (user_preferences[user_id]['content_mode'] + 1) % 3
    await event.respond(f"Content mode changed to {CONTENT_MODE[user_preferences[user_id]['content_mode']]}")


@client.on(events.NewMessage)
async def message_handler(event):
    """Handle all text messages"""
    if event.message.text.startswith('/'):
        return

    start_time = time.time()
    try:
        user_id = event.sender_id
        if user_id not in user_preferences:
            user_preferences[user_id] = {'nsfw_mode': True, 'content_mode': 0}
            
        tags, exclude_tags, page_id, limit = process_user_message(event.message.text)

        if user_preferences[user_id]['content_mode'] == 0:
            exclude_tags.append("video")
        elif user_preferences[user_id]['content_mode'] == 1:
            tags.append("video")
        else:
            pass

        if not user_preferences[user_id]['nsfw_mode']:
            tags.append("rating:safe")

        search = await searching_process(tags, exclude_tags, page_id, limit)
        files_processed = await sending_file(event, search)

        elapsed_time = time.time() - start_time
        last_page_id = page_id + limit
        await event.reply(
            f"Done in {elapsed_time:.2f} seconds, downloaded {files_processed} file(s).\n"
            f"Last processed page ID: {last_page_id}\n"
            f"Last processed query: {event.message.text}", buttons=[Button.inline("Search again", b"search")]
        )
    except Exception as e:
        await event.respond(f"An error occurred: {str(e)}")

@client.on(events.CallbackQuery)
async def callback_query(event):
    if event.data == b'search':
        # Retrieve the original message that was replied to
        original_message = await event.get_message()
        if original_message and original_message.reply_to:
            # Get the replied-to message's content
            reply_message = await original_message.get_reply_message()

            if reply_message:
                start_time = time.time()
                try:
                    user_id = event.sender_id
                    if user_id not in user_preferences:
                        user_preferences[user_id] = {'nsfw_mode': True, 'content_mode': 0}
                        
                    # Extract the last processed query and page ID from the bot's message
                    message_lines = original_message.text.split('\n')
                    last_page_id = int(message_lines[-2].replace('Last processed page ID: ', ''))
                    last_query = message_lines[-1].replace('Last processed query: ', '')

                    # Process the message with the extracted information
                    tags, exclude_tags, _, limit = process_user_message(last_query)

                    # Apply content and NSFW mode logic
                    if user_preferences[user_id]['content_mode'] == 0:
                        exclude_tags.append("video")
                    elif user_preferences[user_id]['content_mode'] == 1:
                        tags.append("video")

                    if not user_preferences[user_id]['nsfw_mode']:
                        tags.append("rating:safe")

                    # Perform the searching process and send files
                    search = await searching_process(tags, exclude_tags, last_page_id, limit)
                    files_processed = await sending_file(event, search)

                    elapsed_time = time.time() - start_time
                    new_last_page_id = last_page_id + limit

                    # Send the response with updated information
                    await event.reply(
                        f"Done in {elapsed_time:.2f} seconds. Downloaded {files_processed} file(s).\n"
                        f"Last processed page ID: {new_last_page_id}\n"
                        f"Last processed query: {last_query}",
                        buttons=[Button.inline("Search again", b"search")]
                    )
                except Exception as e:
                    await event.respond(f"An error occurred: {str(e)}")


async def searching_process(tags: List[str], exclude_tags: List[str], page_id: int, limit: int):
    """Search for content using Rule34"""
    async with AsyncRule34() as r34:
        print("Searching...")
        print(f"Tags: {tags}, Exclude tags: {exclude_tags}, Page ID: {page_id}, Limit: {limit}")
        return await r34.search(tags=tags, exclude_tags=exclude_tags, page_id=page_id, limit=limit)


def chunkify(lst: List, size: int):
    """Split a list into smaller lists (chunks) of the given size."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


async def download_file(url: str, file_path: str) -> bool:
    """Download a file from URL to specified path"""
    try:
        session = await get_session()
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Failed to fetch file: {url}. Status code: {response.status}")
                return False

            with open(file_path, 'wb') as f:
                while True:
                    chunk = await response.content.read(8192)
                    if not chunk:
                        break
                    f.write(chunk)
            return True
    except Exception as e:
        print(f"Error downloading file {url}: {e}")
        return False


async def process_single_file(event, url: str, file_extension: str, file_size: int, temp_file_path: str,
                              downloaded_files: list):
    """Process a single file for sending"""
    try:
        sem = await get_semaphore()
        async with sem:  # Limit concurrent operations
            if file_extension in ['.jpg', '.jpeg', '.png', '.gif']:
                if url.lower().endswith('.jpeg.jpg'):
                    try:
                        async with client.action(event.chat_id, 'photo') as action:
                            await client.send_file(
                                event.chat_id,
                                url,
                                force_document=False,
                                progress_callback=action.progress
                            )
                            return None  # Successfully sent directly
                    except Exception as e:
                        if "Webpage media empty" in str(e):
                            print(f"Skipping empty media: {url}")
                            return None
                        print(f"Direct URL sending failed, downloading file: {e}")
                        if await download_file(url, temp_file_path):
                            downloaded_files.append(temp_file_path)
                            async with client.action(event.chat_id, 'photo') as action:
                                await client.send_file(
                                    event.chat_id,
                                    temp_file_path,
                                    force_document=False,
                                    progress_callback=action.progress
                                )
                            return None
                elif file_size <= MAX_GROUP_SIZE:  # 10MB
                    return url  # Return URL for group sending
                else:
                    try:
                        async with client.action(event.chat_id, 'document') as action:
                            await client.send_file(
                                event.chat_id,
                                url,
                                force_document=True,
                                progress_callback=action.progress
                            )
                            return None
                    except Exception as e:
                        if "Webpage media empty" in str(e):
                            print(f"Skipping empty media: {url}")
                            return None
                        print(f"Direct URL sending failed, downloading file: {e}")
                        if await download_file(url, temp_file_path):
                            downloaded_files.append(temp_file_path)
                            async with client.action(event.chat_id, 'document') as action:
                                await client.send_file(
                                    event.chat_id,
                                    temp_file_path,
                                    force_document=True,
                                    progress_callback=action.progress
                                )
                            return None

            elif file_extension in ['.mp4', '.mov', '.avi', '.mkv']:
                if file_size <= MAX_SINGLE_FILE:  # 50MB
                    try:
                        async with client.action(event.chat_id, 'video') as action:
                            await client.send_file(
                                event.chat_id,
                                url,
                                force_document=False,
                                progress_callback=action.progress
                            )
                            return None
                    except Exception as e:
                        if "Webpage media empty" in str(e):
                            print(f"Skipping empty video: {url}")
                            return None
                        print(f"Direct URL sending failed, downloading file: {e}")
                        if await download_file(url, temp_file_path):
                            downloaded_files.append(temp_file_path)
                            async with client.action(event.chat_id, 'video') as action:
                                await client.send_file(
                                    event.chat_id,
                                    temp_file_path,
                                    force_document=False,
                                    progress_callback=action.progress
                                )
                            return None
                else:
                    try:
                        async with client.action(event.chat_id, 'document') as action:
                            await client.send_file(
                                event.chat_id,
                                url,
                                force_document=True,
                                progress_callback=action.progress
                            )
                            return None
                    except Exception as e:
                        if "Webpage media empty" in str(e):
                            print(f"Skipping empty video: {url}")
                            return None
                        print(f"Direct URL sending failed, downloading file: {e}")
                        if await download_file(url, temp_file_path):
                            downloaded_files.append(temp_file_path)
                            async with client.action(event.chat_id, 'document') as action:
                                await client.send_file(
                                    event.chat_id,
                                    temp_file_path,
                                    force_document=True,
                                    progress_callback=action.progress
                                )
                            return None

    except Exception as e:
        if "Webpage media empty" in str(e):
            print(f"Skipping empty media: {url}")
            return None
        print(f"Error processing file {url}: {e}")
        try:
            if await download_file(url, temp_file_path):
                downloaded_files.append(temp_file_path)
                async with client.action(event.chat_id, 'document') as action:
                    await client.send_file(
                        event.chat_id,
                        temp_file_path,
                        force_document=True,
                        progress_callback=action.progress
                    )
                return None
        except Exception as e:
            print(f"Error sending document: {e}")
            return None

    print(f"Processing: {url}")
    return None


async def sending_file(event, search_results):
    """Send files from search results to Telegram with concurrent processing"""
    downloaded_files = []  # Keep track of downloaded files for cleanup
    tasks = []  # Keep track of concurrent tasks
    group_media = []  # Keep track of media for group sending

    for group in chunkify(search_results, 10):
        file_tasks = []
        for file in group:
            url = file.file_url
            file_extension = os.path.splitext(url)[1].lower()
            temp_file_path = os.path.join(TEMP_DIR, f"{int(time.time())}_{len(downloaded_files)}{file_extension}")

            # Get file size
            file_size = await get_file_size(url)
            if file_size is None:
                print(f"Could not get file size for {url}")
                continue

            # Create task for processing this file
            task = asyncio.create_task(
                process_single_file(event, url, file_extension, file_size, temp_file_path, downloaded_files)
            )
            file_tasks.append(task)

        # Wait for all files in this group to be processed
        results = await asyncio.gather(*file_tasks, return_exceptions=True)

        # Collect URLs for group sending
        group_media.extend([r for r in results if r is not None])

        # If we have enough media for a group or this is the last group, send it
        if len(group_media) >= 10 or group == list(chunkify(search_results, 10))[-1]:
            if group_media:
                try:
                    async with client.action(event.chat_id, 'photo') as action:
                        await client.send_file(
                            event.chat_id,
                            group_media[:10],  # Send maximum 10 at a time
                            force_document=False,
                            progress_callback=action.progress
                        )
                except Exception as e:
                    print(f"Error sending media group: {e}")
                    # If media group fails, try sending files individually
                    for media in group_media[:10]:
                        try:
                            async with client.action(event.chat_id, 'document') as action:
                                await client.send_file(
                                    event.chat_id,
                                    media,
                                    force_document=False,
                                    progress_callback=action.progress
                                )
                        except Exception as e:
                            print(f"Error sending individual document: {e}")

                group_media = group_media[10:]  # Keep remaining media for next group

    # Cleanup: Delete all downloaded files
    try:
        for file_path in downloaded_files:
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")

        # Remove the temporary directory
        await cleanup()
        print("Cleanup completed successfully")
    except Exception as e:
        print(f"Error during cleanup: {e}")

    print("All files processed successfully")
    return len(search_results)  # Return the number of files processed


def process_user_message(message: str) -> Tuple[List[str], List[str], int, int]:
    """Process user message to extract tags and parameters."""
    # Updated pattern for tags to include underscores as valid characters
    tag_pattern = r'(-?[a-zA-Z0-9._\s()]+(?:\([\w\s.]+\))?)'  # Include _ in tags
    pid_pattern = r'\bpid\s*(\d+)'  # Match 'pid' parameter
    limit_pattern = r'\blimit\s*(\d+)'  # Match 'limit' parameter

    # Extract 'pid' and 'limit' first
    pid_match = re.search(pid_pattern, message)
    pid = int(pid_match.group(1)) if pid_match else 0

    limit_match = re.search(limit_pattern, message)
    limit = int(limit_match.group(1)) if limit_match else 10

    # Remove 'pid' and 'limit' entirely from the message before extracting tags
    cleaned_message = re.sub(pid_pattern, '', message)
    cleaned_message = re.sub(limit_pattern, '', cleaned_message)

    # Extract all potential tags from the cleaned message
    tags_match = re.findall(tag_pattern, cleaned_message)

    # Filter tags to exclude 'pid' or 'limit' (case-insensitive match) and clean outputs
    tags = [tag.strip() for tag in tags_match if tag.strip().lower() not in {'pid', 'limit'}]

    # Separate include and exclude tags
    include_tags = [
        tag.replace(" ", "_").lower()
        for tag in tags if not tag.startswith('-')
    ]  # Replace spaces with underscores, preserve existing underscores, dots, and parentheses
    exclude_tags = [
        tag.lstrip('-').replace(" ", "_").lower()
        for tag in tags if tag.startswith('-')
    ]

    # Limit the number of tags to 3 each
    include_tags = include_tags[:3]
    exclude_tags = exclude_tags[:3]

    return include_tags, exclude_tags, pid, limit



async def get_file_size(url: str) -> Optional[int]:
    """Get file size from URL"""
    try:
        session = await get_session()
        async with session.head(url, allow_redirects=True) as response:
            content_length = response.headers.get("Content-Length")
            return int(content_length) if content_length else None
    except Exception as e:
        print(f"Failed to fetch file size: {e}")
        return None


async def cleanup():
    """Cleanup resources"""
    global session
    if session and not session.closed:
        await session.close()
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)


def main():
    """Start the bot"""
    print("Bot started...")
    try:
        client.run_until_disconnected()
    finally:
        # Ensure cleanup happens even if the bot crashes
        asyncio.run(cleanup())


if __name__ == "__main__":
    main()
