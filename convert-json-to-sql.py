import json
import psycopg2
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def convert_to_datetime(date_str):
    """Convert ISO 8601 string to datetime object."""
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        logging.warning(f"Invalid date format: {date_str}")
        return None

def convert_objectid(objectid):
    """Convert MongoDB ObjectId to a string."""
    return objectid.get('$oid', '') if isinstance(objectid, dict) else objectid

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect("dbname=burat user=burat password=burat host=127.0.0.1")
    cur = conn.cursor()

    logging.info("Connected to PostgreSQL")

    # Open the JSON file
    with open('data_array.json', 'r') as file:
        data = json.load(file)

    logging.info("JSON data loaded successfully")

    # Insert data into PostgreSQL
    for item in data:
        # Convert types if necessary
        item['photo_submitted_at'] = convert_to_datetime(item.get('photo_submitted_at', ''))
        item['photo_featured'] = item.get('photo_featured', False)
        
        item['mongo_id'] = convert_objectid(item.get('_id', {}))

        insert_data = {
            'photo_id': item.get(' photo_id', ''),
            'photo_url': item.get('photo_url', ''),
            'photo_image_url': item.get('photo_image_url', ''),
            'photo_submitted_at': item['photo_submitted_at'],
            'photo_featured': item['photo_featured'],
            'photo_width': item.get('photo_width', 0),
            'photo_height': item.get('photo_height', 0),
            'photo_aspect_ratio': item.get('photo_aspect_ratio', 0.0),
            'photographer_username': item.get('photographer_username', ''),
            'photographer_first_name': item.get('photographer_first_name', ''),
            'photographer_last_name': item.get('photographer_last_name', ''),

            'photo_location_name': item.get('photo_location_name', ''),
            'photo_location_latitude': item.get('photo_location_latitude', 0.0),
            'photo_location_longitude': item.get('photo_location_longitude', 0.0),
            'photo_location_country': item.get('photo_location_country', ''),
            'photo_location_city': item.get('photo_location_city', ''),

            'exif_camera_make': item.get('exif_camera_make', ''),
            'exif_camera_model': item.get('exif_camera_model', ''),
            'exif_iso': item.get('exif_iso', ''),
            'exif_aperture_value': item.get('exif_aperture_value', 0.0),
            'exif_focal_length': item.get('exif_focal_length', 0.0),
            'exif_exposure_time': item.get('exif_exposure_time', ''),
            'stats_views': item.get('stats_views', 0),
            'stats_downloads': item.get('stats_downloads', 0),
            'ai_description': item.get('ai_description', ''),
            'blur_hash': item.get('blur_hash', ''),
        }

        try:
            cur.execute("""
                INSERT INTO photos (
                    photo_id, photo_url, photo_image_url, photo_submitted_at,
                    photo_featured, photo_width, photo_height, photo_aspect_ratio, 
                    photographer_username, photographer_first_name, photographer_last_name,
                    exif_camera_make, exif_camera_model, exif_iso, exif_aperture_value,
                    exif_focal_length, exif_exposure_time, stats_views, stats_downloads,
                    ai_description, blur_hash, photo_location_name, photo_location_latitude,
                    photo_location_longitude, photo_location_country, photo_location_city
                ) VALUES (
                    %(photo_id)s, %(photo_url)s, %(photo_image_url)s, %(photo_submitted_at)s,
                    %(photo_featured)s, %(photo_width)s, %(photo_height)s, %(photo_aspect_ratio)s,
                    %(photographer_username)s, %(photographer_first_name)s, %(photographer_last_name)s,
                    %(exif_camera_make)s, %(exif_camera_model)s, %(exif_iso)s, %(exif_aperture_value)s,
                    %(exif_focal_length)s, %(exif_exposure_time)s, %(stats_views)s, %(stats_downloads)s,
                    %(ai_description)s, %(blur_hash)s, %(photo_location_name)s, %(photo_location_latitude)s,
                    %(photo_location_longitude)s, %(photo_location_country)s, %(photo_location_city)s
                )
            """, insert_data)
        except psycopg2.Error as e:
            logging.error(f"Error inserting data: {e}")
            conn.rollback()  # Rollback the transaction if an error occurs
            continue

    # Commit and close connection
    conn.commit()
    logging.info("Data committed successfully")

except psycopg2.Error as e:
    logging.error(f"Database connection error: {e}")

finally:
    if conn is not None:
        cur.close()
        conn.close()
        logging.info("Database connection closed")



