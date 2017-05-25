import requests
import logging
import json
import time
from typing import Generator
from datetime import date, datetime, timedelta


# 401 -> get new TT


class GreyPoupon(object):
    def __init__(self, server: str, sst: str = None) -> None:
        self.base_url = 'https://%s.gooddata.com' % server
        self.server = server

        if sst:
            self.temp_token = self._get_tt(sst=sst)
        else:
            self.temp_token = None

    def authenticate(self,
                     sst: str = None,
                     user: str = None,
                     password: str = None) -> None:
        """
        Generates a temporary token using either a pre-existing
        super secure token or the user/password combination for
        a user.
        
        :param sst: 
        :param user: 
        :param password: 
        :return: 
        """
        if sst:
            self.temp_token = self._get_tt(sst=sst)
        elif user and password:
            self.temp_token = self._get_tt(
                sst=self._get_sst(
                    user=user,
                    password=password,
                    remember=0,
                    verify_level=2
                )
            )
        else:
            raise Exception('SST or User/Password must be provided!')

    def _get_sst(self,
                 user: str,
                 password: str,
                 remember: bool = False,
                 verify_level: int = 2) -> str:
        """
        Get a super secure token from GoodData.
        
        :param remember: False for session-based or True for longer
        :param verify_level: Specifies how the SST should 
        be returned back to the client. Can be set to 0 
        (HTTP cookie - GDCAuthSST) or 2 (custom HTTP 
        header - X-GDC-AuthSST)
        :return: 
        """
        url = self.base_url + '/gdc/account/login'
        body = {
            "postUserLogin": {
                "login": user,
                "password": password,
                "remember": int(remember),
                "verify_level": verify_level
            }
        }
        res = requests.post(url, data=json.dumps(body))

        if res.status_code == 200:
            print(res.json())
            return res.json().get('userLogin').get('token')
        elif res.status_code == 429:
            raise Exception(
                'Too many invalid login requests. '
                'Please check your credentials and try again in 60 sec.'
            )
            # look for Retry-After HTTP  in the header response
        else:
            print(res.text)
            raise Exception(res.status_code)

    def _get_tt(self, sst: str) -> str:
        """
        Get a temporary token from GoodData.
        :return: 
        """
        url = self.base_url + '/gdc/account/token'
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-GDC-AuthSST': sst
        }
        res = requests.get(url=url, headers=headers)

        if res.status_code == 200:
            print(res.json())
            return res.json().get('userToken').get('token')
        else:
            print(res.text)
            raise Exception(res.status_code)

    def get_cookie(self) -> str:
        """
        
        :return: 
        """
        return 'GDCAuthTT=%s' % self.temp_token

    def headers(self) -> dict:
        """
        
        :return: 
        """
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cookie': self.get_cookie()
        }
        return headers

    def list_metrics(self, project_id: str) -> Generator[str, None, None]:
        """
        
        :param project_id: 
        :return: 
        """
        url = '{base}/gdc/md/{project_id}/query/metrics'
        res = requests.get(
            url=url.format(base=self.base_url, project_id=project_id),
            headers=self.headers()
        )
        for metric in res.json().get('query').get('entries'):
            yield metric

    def download_list_of_metrics(self, project_id, download_path):
        """
        
        :param project_id: 
        :param download_path: 
        :return: 
        """
        url = '{base}/gdc/md/{project_id}/query/metrics'
        res = requests.get(
            url=url.format(base=self.base_url, project_id=project_id),
            headers=self.headers()
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
            headers=self.headers(),
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
        res = requests.get(url, headers=self.headers())
        if res.status_code == 200:
            status = res.json().get('taskState').get('status')
            msg = res.json().get('taskState').get('msg')
            print(msg)
            return status == 'OK'
        else:
            print(res.text)
            raise Exception(res.status_code)

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
            headers=self.headers(),
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
            headers=self.headers(),
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
            headers=self.headers()
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

    def export_object(self,
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
            data=body,
            headers=self.headers()
        )

        if res.status_code == 200:
            return res.json()
        else:
            print(res.text)
            raise Exception(res.status_code)

    def import_object(self,
                      project_id: str,
                      token: str,
                      overwrite_newer: bool = True,
                      update_ldm_objects: bool = True,
                      import_attribute_properties: bool = True):
        """
        
        :param project_id: 
        :param token: 
        :param overwrite_newer: 
        :param update_ldm_objects: 
        :param import_attribute_properties: 
        :return: 
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
            data=body,
            headers=self.headers()
        )

        if res.status_code == 200:
            return res.json()
        else:
            print(res.text)
            raise Exception(res.status_code)