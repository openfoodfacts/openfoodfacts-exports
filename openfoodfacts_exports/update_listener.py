import logging
import time

import backoff
from openfoodfacts import Environment, Flavor
from openfoodfacts.redis import ProductUpdateEvent
from openfoodfacts.redis import UpdateListener as BaseUpdateListener
from redis import Redis
from redis.exceptions import ConnectionError

from openfoodfacts_exports import settings
from openfoodfacts_exports.tasks import delete_image_from_s3, upload_new_image_to_s3

logger = logging.getLogger(__name__)


def get_redis_client():
    """Get the Redis client where Product Opener publishes its product updates."""
    return Redis(
        host=settings.REDIS_UPDATE_HOST,
        port=settings.REDIS_UPDATE_PORT,
        decode_responses=True,
    )


class UpdateListener(BaseUpdateListener):
    def process_redis_update(self, event: ProductUpdateEvent):
        logger.debug("New update: %s", event)

        if not event.code:
            logger.warning("Product code is empty or null ('%s'), skipping", event.code)
            return

        action = event.action
        if action == "deleted":
            logger.info("Product %s has been deleted", event.code)
        elif action == "updated":
            if event.is_image_upload():
                self.process_image_upload(event)
            elif event.is_image_deletion():
                self.process_image_deletion(event)

    def process_image_upload(self, event: ProductUpdateEvent):
        # A new image was uploaded
        image_id = event.diffs["uploaded_images"]["add"][0]  # type: ignore
        logger.info("Image %s was added on product %s", image_id, event.code)
        environment = (
            Environment.org if settings.ENVIRONMENT == "prod" else Environment.net
        )
        flavor = Flavor[event.flavor]

        # The redis event is sometimes published before Product Opener finishes
        # to process the uploaded image, so we wait 2 seconds before processing
        # the upload
        if time.time() - event.timestamp.timestamp() < 2:
            logger.info("Waiting 2 seconds before processing the upload")
            time.sleep(2)

        upload_new_image_to_s3(
            image_id=image_id,
            barcode=event.code,
            flavor=flavor,
            environment=environment,
        )

    def process_image_deletion(self, event: ProductUpdateEvent):
        image_id = event.diffs["uploaded_images"]["delete"][0]  # type: ignore
        logger.info(
            "Image %s for product %s has been deleted",
            image_id,
            event.code,
        )
        delete_image_from_s3(image_id=image_id, barcode=event.code)


@backoff.on_exception(
    backoff.expo,
    ConnectionError,
    max_value=60,  # we wait at most 60 seconds between retries
    jitter=backoff.random_jitter,
    on_backoff=lambda details: logger.error(
        "Redis connection error (attempt %d): %s. Retrying in %.1f seconds...",
        details["tries"],
        details["exception"],
        details["wait"],
    ),
    on_giveup=lambda details: logger.critical(
        "Max retries (%d) reached. Update listener is terminating.", details["tries"]
    ),
)
def run_update_listener():
    """Run the update import daemon.

    This daemon listens to the Redis stream containing information about
    product updates and triggers appropriate actions.
    """
    logger.info("Starting Redis update listener...")
    while True:
        try:
            redis_client = get_redis_client()
            update_listener = UpdateListener(
                redis_client=redis_client,
                redis_latest_id_key=settings.REDIS_LATEST_ID_KEY,
                product_updates_stream_name=settings.PRODUCT_UPDATE_STREAM_NAME,
            )
            update_listener.run()
        except Exception as e:
            logger.critical(
                "Unexpected error in update listener: %s", str(e), exc_info=True
            )
            raise
