import requests
import logging
import json
import time
from typing import Generator
from datetime import date, datetime, timedelta

from .http_errors import *

logging.basicConfig(level=logging.INFO)

# TODO
# 401 -> get new TT
# remove print stmts and replace with logging
# handle all status != 200

# product/version (your_email@example.com)


class AuthenticationProblem(Exception):
    def __init__(self, expression, status, body):
        self.expression = expression
        self.message = "Authentication problem.\nstatus: %s\nbody:\n%s" % (
            status, body)


class CredentialsMissing(Exception):
    def __init__(self):
        self.message = 'SST or User/Password must be ' \
                       'provided to "authenticate(...)" method!'


class GreyPoupon(object):
    """
    Python wrapper of the GoodData REST API. Basically the python version
    of Gray Pages interface.

    Use: initiate the object with your organization sub-domain and,
    optionally an super secure token (SST).

    If SST is provided during init, authentication is done automatically,
    that is, a temporary token is requested and saved as an attribute of
    your object. All further request to the GD API will use this temporary
    token.

    If SST is not provided, user must call the authenticate() method.

    ....

    """

    def __init__(self, sub_domain: str, sst: str = None) -> None:
        self.base_url = 'https://%s.gooddata.com' % sub_domain
        self.sub_domain = sub_domain

        if sst:
            self.temp_token = self._get_tt(sst=sst)
        else:
            self.temp_token = None

    @property
    def auth_cookie(self) -> str:
        """
        Returns the authentication cookie to be added
        into the header of each request.
        """
        return 'GDCAuthTT=%s' % self.temp_token

    @property
    def headers(self) -> dict:
        """
        Returns the basic header needed for each request, including
        the authentication cookie.
        """
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cookie': self.auth_cookie
        }
        return headers

    def authenticate(self,
                     sst: str = None,
                     user: str = None,
                     password: str = None) -> None:
        """
        Generates a temporary token (TT) using either a pre-existing
        super secure token (SST) or the user/password combination for
        a user.
        
        :param sst: super secure token
        :param user: GoodData login email/alias
        :param password: GoodData login password
        :return: Nothing. self.temp_token will be updated.
        """
        if sst:
            self.temp_token = self._get_tt(sst=sst)
        elif user and password:
            self.temp_token = self._get_tt(
                sst=self._get_sst(
                    user=user,
                    password=password,
                    remember=False,
                    verify_level=2
                )
            )
        else:
            raise CredentialsMissing()

    def _get_sst(self,
                 user: str,
                 password: str,
                 remember: bool = False,
                 verify_level: int = 2) -> str:
        """
        Get a super secure token (SST) from GoodData using your
        login credentials.
        
        :param remember: False for session-based token
        or True for longer lasting token
        :param verify_level: Specifies how the SST should
        be returned back to the client. Can be set to 0 
        (HTTP cookie - GDCAuthSST) or 2 (custom HTTP 
        header - X-GDC-AuthSST)
        :return: the super secure token (SST)
        """
        url = self.base_url + '/gdc/account/login'
        body = {
            "postUserLogin": {
                "login": user,
                "password": password,
                "remember": remember,
                "verify_level": verify_level
            }
        }
        res = requests.post(url, data=json.dumps(body))

        if res.status_code == 200:
            return res.json().get('userLogin').get('token')
        elif res.status_code == 429:
            raise TooManyRequests('POST: %s' % url)
        else:
            raise AuthenticationProblem(
                expression='POST: %s' % url,
                status=res.status_code,
                body=res.text
            )

    def _get_tt(self, sst: str) -> str:
        """
        Request a temporary token from GoodData using
        a super secure token (SST).

        :param: sst: super secure token (SST)
        :return: temporary token (TT)
        """
        url = self.base_url + '/gdc/account/token'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-GDC-AuthSST': sst
        }
        res = requests.get(url=url, headers=headers)

        if res.status_code == 200:
            return res.json().get('userToken').get('token')
        else:
            raise AuthenticationProblem(
                expression='GET: %s' % url,
                status=res.status_code,
                body=res.text
            )

    def list_metrics(self, project_id: str) -> Generator[str, None, None]:
        """
        Generates a list of metrics within a project.

        :param project_id: ID of the project for which you want to
        get the list of metrics
        :return: list of metrics is yielded
        """
        url = '{base}/gdc/md/{project_id}/query/metrics'
        res = requests.get(
            url=url.format(base=self.base_url, project_id=project_id),
            headers=self.headers
        )
        for metric in res.json().get('query').get('entries'):
            if metric.get('category') == 'metric':
                yield metric

    def download_list_of_metrics(self,
                                 project_id: str,
                                 download_path: str) -> None:
        """
        Download the JSON response into a file.

        :param project_id: ID of the project for which to get
        the metrics
        :param download_path: path to JSON file where
        to store the response
        :return: 
        """
        url = '{base}/gdc/md/{project_id}/query/metrics'
        res = requests.get(
            url=url.format(base=self.base_url, project_id=project_id),
            headers=self.headers
        )

        with open(download_path, 'w') as download_file:
            json.dump(res.json(), fp=download_file)

    def export_project(self,
                       project_id: str,
                       include_users: bool = False,
                       include_data: bool = False,
                       include_schedules: bool = False,
                       cross_data_center_export: bool = False) -> tuple:
        """
        
        :param project_id: 
        :param include_users: 
        :param include_data: 
        :param include_schedules: 
        :param cross_data_center_export: 
        :return: status uri and export token
        """
        url = '{base}/gdc/md/{project_id}/maintenance/export'
        body = {
            "exportProject": {
                "exportUsers": int(include_users),
                "exportData": int(include_data),
                "excludeSchedules": int(include_schedules),
                "crossDataCenterExport": int(cross_data_center_export)
            }
        }
        res = requests.post(
            url=url.format(base=self.base_url, project_id=project_id),
            headers=self.headers,
            data=json.dumps(body)
        )

        if res.status_code == 200:
            status_uri = res.json().get('exportArtifact').get('status').get('uri')
            token = res.json().get('exportArtifact').get('token')
            return status_uri, token
        else:
            print(res.text)
            raise Exception(res.status_code)

    def is_export_done(self, status_uri: str) -> bool:
        """
        
        :param status_uri: 
        :return: 
        """
        url = self.base_url + status_uri
        res = requests.get(url, headers=self.headers)
        if res.status_code == 200:
            status = res.json().get('wTaskStatus').get('status')
            return status == 'OK'
        else:
            print(res.text)

    def import_project(self, project_id: str, token: str) -> str:
        """
        
        :param project_id: 
        :param token: 
        :return: 
        """
        url = '{base}/gdc/md/{project_id}/maintenance/import'
        body = {
            "importProject": {
                "token": token,
            }
        }
        res = requests.post(
            url=url.format(base=self.base_url, project_id=project_id),
            headers=self.headers,
            data=json.dumps(body)
        )
        if res.status_code == 200:
            return res.json().get('uri')
        else:
            print(res.text)
            raise Exception(res.status_code)

    def create_project(self,
                       token: str,
                       title: str,
                       summary: str = None,
                       db: str = 'Pg',
                       environment: str = 'DEVELOPMENT') -> str:
        """
                
        :param token: 
        :param title: 
        :param summary:
        :param db: Database type: "Pg" or ??
        :param environment: PRODUCTION, DEVELOPMENT, TESTING
        :return: 
        """
        url = '{base}/gdc/projects'

        project_template = {
            "projectTemplate": "/projectTemplates/{name}/{version}"
        }

        body = {
            "project": {
                "content": {
                    "guidedNavigation": 1,
                    "driver": db,
                    "authorizationToken": token,
                    "environment": environment
                },
                "meta": {
                    "title": title,
                    "summary": summary
                }
            }
        }
        res = requests.post(
            url=url.format(base=self.base_url),
            headers=self.headers,
            data=json.dumps(body)
        )
        if res.status_code == 200:
            return res.json().get('uri').split('/')[-1]
        else:
            print(res.text)
            raise Exception(res.status_code)

    def get_project_information(self, project_id: str) -> dict:
        """
        
        :param project_id: 
        :return: 
        """
        url = '{base}/gdc/projects/{project_id}'
        res = requests.post(
            url=url.format(base=self.base_url, project_id=project_id),
            headers=self.headers
        )
        if res.status_code == 200:
            return res.json().get('project')
        else:
            print(res.text)
            raise Exception(res.status_code)

    def get_project_state(self, project_id: str) -> str:
        """
        
        :param project_id: 
        :return: 
        """
        info = self.get_project_information(project_id)
        return info.get('content').get('state')

    def backup_project(self,
                       project_id: str,
                       create_project_token: str,
                       include_users: bool = False,
                       include_data: bool = False,
                       include_schedules: bool = False) -> str:
        """
        
        :param project_id: 
        :param include_users: 
        :param include_data: 
        :param include_schedules: 
        :return: 
        """
        info = self.get_project_information(project_id=project_id)
        title = info.get('meta').get('title')
        environment = info.get('content').get('environment')
        today = date.isoformat(date.today())
        print('start export')

        status_uri, token = self.export_project(
            project_id=project_id,
            include_users=include_users,
            include_data=include_data,
            include_schedules=include_schedules
        )

        bkp_pid = self.create_project(
            token=create_project_token,
            title='Backup%s %s' % (today, title),
            environment=environment,
        )

        while not self.is_export_done(status_uri):
            time.sleep(1)

        while not self.get_project_state(project_id) == 'ENABLED':
            time.sleep(1)

        status_uri = self.import_project(project_id=bkp_pid, token=token)

        while not self.is_export_done(status_uri):
            time.sleep(1)

        return bkp_pid

    def export_objects(self,
                       project_id: str,
                       object_uris: list,
                       export_attribute_properties: bool = True,
                       cross_datacenter_export: bool = False):
        """
        
        :param project_id: ID of the source project 
        :param object_uris: List of objects to be exported, each object
        will be represented by the GoodData uri
        :param export_attribute_properties: Specifies whether to include 
        drill-down attribute settings and label types in partial export. 
        :param cross_datacenter_export: Specifies whether export can be 
        used in any datacenter.
        :return: 
        """
        url = '{base}/gdc/md/{project_id}/maintenance/partialmdexport'
        body = {
            "partialMDExport": {
                "uris": object_uris,
                "exportAttributeProperties": export_attribute_properties,
                "crossDataCenterExport": cross_datacenter_export
            }
        }

        res = requests.post(
            url=url.format(base=self.base_url, project_id=project_id),
            data=json.dumps(body),
            headers=self.headers
        )

        if res.status_code == 200:
            status_uri = res.json().get('partialMDArtifact').get('status').get('uri')
            token = res.json().get('partialMDArtifact').get('token')
            return status_uri, token
        else:
            print(res.text)
            raise Exception(res.status_code)

    def import_objects(self,
                       project_id: str,
                       token: str,
                       overwrite_newer: bool = True,
                       update_ldm_objects: bool = True,
                       import_attribute_properties: bool = True):
        """
        Import objects into the specified project. Returns the task
        uri to be checked if the import ran successfully.

        :param project_id: ID of the project where to import the objects
        :param token: token returned by the export_objects method
        :param overwrite_newer: overwrite if object already exists
        :param update_ldm_objects: update ldm objects
        :param import_attribute_properties: import drill-down attribute
        settings and label types from the partial export
        :return: Task URI
        """
        url = '{base}/gdc/md/{project_id}/maintenance/partialmdimport'
        body = {
            "partialMDImport": {
                "token": token,
                "overwriteNewer": overwrite_newer,
                "updateLDMObjects": update_ldm_objects,
                "importAttributeProperties": import_attribute_properties
            }
        }

        res = requests.post(
            url=url.format(base=self.base_url, project_id=project_id),
            data=json.dumps(body),
            headers=self.headers
        )

        if res.status_code == 200:
            return res.json().get('uri')
        else:
            print(res.text)
            raise Exception(res.status_code)

    def delete_objects(self,
                       project_id: str,
                       object_uris: list):
        url = '{base}{object_uri}'

        for object_uri in object_uris:
            res = requests.delete(
                url=url.format(base=self.base_url, object_uri=object_uri),
                headers=self.headers
            )
            if not res.status_code == 204:
                print(res.status_code, res.text)