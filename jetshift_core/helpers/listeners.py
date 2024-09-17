def listen(channel, handle_message, timeout=1, poll_interval=0.01):
    import time
    from config.logging import logger
    from config.database import redis_connection

    redis_conn = redis_connection()

    mobile = redis_conn.pubsub()
    mobile.subscribe(channel)
    logger.info(f'Subscribed to channel: {channel}. Listening for events (Press Ctrl + C to stop).')

    try:
        while True:
            message = mobile.get_message(timeout=timeout)
            if message:
                handle_message(message)  # Call the passed-in message handler
            time.sleep(poll_interval)  # Small sleep to prevent a busy-wait loop
    except KeyboardInterrupt:
        logger.info("Stopping the listener...")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if mobile:
            mobile.close()
        redis_conn.close()
        logger.info("Redis connection closed.")
