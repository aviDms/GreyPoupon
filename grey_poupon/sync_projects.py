import time
import logging
from .client import GreyPoupon


def sync_metrics(client: GreyPoupon,
                 master_pid: str,
                 slave_pid: str,
                 tag: str) -> None:
    """
    Sync metric definition from a master workspace to a slave workspace.

    :param client: GreyPoupon connection to GoodData API
    :param master_pid: workspace where metrics are up to date
    :param slave_pid: workspace where metrics will be updated
    :param tag: naming convention to identify metrics belonging to
    the slave workspace
    """
    logging.basicConfig(level=logging.INFO)
    upsert = {}
    delete = {}

    master_metrics = client.list_metrics(project_id=master_pid)
    slave_metrics = client.list_metrics(project_id=slave_pid)

    for metric in master_metrics:
        if tag in metric['tags'].split():
            upsert[metric['identifier']] = metric['link']

    for metric in slave_metrics:
        if tag in metric['tags'].split():
            if metric['identifier'] not in upsert.keys():
                delete[metric['identifier']] = metric['link']

    logging.warning(
        'Following metrics will be '
        'deleted from the %s project: %s' % (
            slave_pid, ', '.join(delete.values())
        )
    )

    client.delete_objects(
        project_id=master_pid,
        object_uris=list(delete.values())
    )

    export_status_uri, token = client.export_objects(
        project_id=master_pid,
        object_uris=list(upsert.values())
    )

    while not client.is_export_done(status_uri=export_status_uri):
        logging.info('Waiting for export to finish ...')
        time.sleep(10)

    logging.info(
        'Following metrics will be added or updated in the '         
        'following project %s from the %s master project: %s' % (
            slave_pid, master_pid, ', '.join(upsert.values())
        )
    )
    import_status_uri = client.import_objects(project_id=slave_pid, token=token)

    while not client.is_export_done(status_uri=import_status_uri):
        logging.info('Waiting for import to finish ...')
        time.sleep(10)

    logging.info('Sync done.')