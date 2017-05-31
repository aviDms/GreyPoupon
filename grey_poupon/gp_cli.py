import os
import sys
import json
import getpass
import click
from grey_poupon import GreyPoupon, sync_metrics

CONFIG_PATH = os.path.join(os.getenv('HOME'), '.config', 'grey_poupon')
LOGIN_FILE = os.path.join(CONFIG_PATH, 'login.json')
CONFIG_SYNC = os.path.join(CONFIG_PATH, 'config_sync.json')


def read_login_file():
    default = {'tokens': {}}
    login_file_is_empty = False

    if os.path.exists(LOGIN_FILE):
        with open(LOGIN_FILE) as login_file:
            data = login_file.read()

        if data:
            return json.loads(data)
        else:
            login_file_is_empty = True

    else:
        os.makedirs(CONFIG_PATH, exist_ok=True)
        login_file_is_empty = True

    if login_file_is_empty:
        with open(LOGIN_FILE, 'w') as login_file:
            json.dump(default, login_file)

    return default


def write_to_login_file(data):
    if not os.path.exists(LOGIN_FILE):
        os.makedirs(CONFIG_PATH)

    with open(LOGIN_FILE, 'w') as login_file:
        json.dump(data, login_file)


def authenticate():
    print('Authenticate workflow for using GoodData\'s API.')
    print('Super Secure Token will be saved in %s' % LOGIN_FILE)

    login_data = read_login_file()

    org_domain = input('Your organization sub-domain in GoodData [company.org]: ')
    user = input('Your GoodData login email or alias for this sub-domain [email@company.org]: ')
    password = getpass.getpass('Your GoodData password for this sub-domain: ')
    # password = input('Your GoodData password for this sub-domain: ')

    client = GreyPoupon(sub_domain=org_domain)
    sst = client._get_sst(user=user, password=password, remember=True)

    login_data['tokens'][org_domain] = sst

    write_to_login_file(login_data)


def read_config_sync_file():
    default = {'workspaces': []}

    if os.path.exists(CONFIG_SYNC):
        with open(CONFIG_SYNC) as config_file:
            data = config_file.read()

        if data:
            return json.loads(data)
        else:
            config_file_is_empty = True

    else:
        os.makedirs(CONFIG_PATH, exist_ok=True)
        config_file_is_empty = True

    if config_file_is_empty:
        with open(CONFIG_SYNC, 'w') as config_file:
            json.dump(default, config_file)

    return default


def write_config_sync_file(data):
    if not os.path.exists(CONFIG_SYNC):
        os.makedirs(CONFIG_PATH)

    with open(CONFIG_SYNC, 'w') as config_file:
        json.dump(data, config_file)


def config_sync():
    print('Create a new sync configuration for your projects.')
    org_domain = input('Your organization sub-domain in GoodData [company.org]: ')
    master = input('ID of the master workspace: ')
    slaves = list()

    add_another_slave = True
    while add_another_slave:
        slave = input('ID of the slave workspace: ')
        tag = input('Tag for metrics what should be updated in the slave workspace: ')

        slaves.append({'slave_pid': slave, 'tag': tag})

        next = input('Add another slave workspace? [y/n]: ')
        if next in ('y', 'Y', 'YES', 'yes'):
            add_another_slave = True
        else:
            add_another_slave = False

    config_file = read_config_sync_file()
    tasks = config_file['workspaces']

    if tasks:
        for task in tasks:
            if task['master_pid'] == master and task['sub_domain'] == org_domain:
                task['slaves'] = slaves
                slaves = list()
                break
    if slaves:
        tasks.append({
            'master_pid': master,
            'sub_domain': org_domain,
            'slaves': slaves
        })

    config_file['workspaces'] = tasks
    write_config_sync_file(config_file)


def sync_metrics_using_config_file():
    logins = read_login_file()
    configs = read_config_sync_file()

    for task in configs['workspaces']:
        sst = logins['tokens'].get(task['sub_domain'], None)
        if sst:
            client = GreyPoupon(sub_domain=task['sub_domain'], sst=sst)
            for slave in task['slaves']:
                print('Sync %s -> %s with tag %s' % (task['master_pid'], slave['slave_pid'], slave['tag']))
                sync_metrics(
                    client=client,
                    master_pid=task['master_pid'],
                    slave_pid=slave['slave_pid'],
                    tag=slave['tag']
                )


@click.command()
@click.option('--auth', is_flag=True, help='Create login configuration file.')
@click.option('--config', is_flag=True, help='Create sync metrics configuration file.')
@click.option('--sync', is_flag=True, help='Sync metrics.')
def gp_cli(auth, config, sync):
    if auth:
        authenticate()

    if config:
        config_sync()

    if sync:
        sync_metrics_using_config_file()
