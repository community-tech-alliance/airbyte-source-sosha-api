#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
from typing import Dict, Generator
import requests
import datetime as dt
import time
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qsl, urlparse


from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode

logger = AirbyteLogger()

class SourceSosha(HttpStream, ABC):
    """Parent class extended by all stream-specific classes
    """

    url_base = "https://share.climatepower.us/"

    def __init__(self, config, **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.url_base = "https://share.climatepower.us/"
        self.yesterday = self._get_unix_date(1)
        self.year_ago = self._get_unix_date(366) #year ago from yesterday, assumes no leap year

    def _get_unix_date(self, timedelta_number):
        start_date = dt.date.today() - dt.timedelta(days = timedelta_number)
        start_date_dt = dt.datetime.fromordinal(start_date.toordinal())
        unix_dt = time.mktime(start_date_dt.timetuple())
        return unix_dt

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {'Authorization': f"Bearer {self.config['api_key']}", 'Content-type': 'application/json'}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        No endpoints of this API currently use pagination, although if we had cause to update to v2 of
        the API, it would support pagination.
        """
        return None

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_slice=stream_slice, next_page_token=next_page_token,stream_state=stream_state)
        params["PageSize"] = self.page_size
        if next_page_token:
           params.update(**next_page_token)
        return params
    
class Campaigns(SourceSosha):
    """
    Returns a list of all campaigns associated with the API key.
    """
    primary_key = "id"
    use_cache = True
    # Use cache so that CampaignInsights streams can use cached stream data instead of making another API call (like in StackAdapt)

    def parse_response(self,
        response: requests.Response, 
        stream_slice: Mapping[str, any] = None,               
        **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """

        return response.json()

    def path(self, **kwargs) -> str:
        return "api/campaign_list"

class CampaignInsights(SourceSosha):
    http_method = "POST"
    primary_key = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.campaigns_stream = Campaigns(**kwargs)

    def _send(self, 
              request: requests.PreparedRequest, 
              request_kwargs: Mapping[str, Any]) -> requests.Response:
        try:
            return super()._send(request, request_kwargs)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("Bad url for this campaign_id")
                response = requests.Response()
                response.status_code = 401
                return response
            else:
                raise e
        
    def parse_response(self,
        response: requests.Response,
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None,) -> Iterable[Mapping]:
        
        status = response.status_code

        if status == 200:
            return [response.json()]
        elif status == 401: 
            return []
        else:
            print("non 401 error occurred")
            return []

    def stream_slices(
        self, 
        sync_mode: SyncMode,
        cursor_field: List[str] = None, 
        stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Create Stream Slices for each Campaign ID.
        """
        for record in self.campaigns_stream.read_records(sync_mode=SyncMode.full_refresh):
            logger.info(f"Slice for Campaign ID: {record['id']}")
            yield {
                "campaign_id": record["id"]
            }

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping[str, Any]]:
        params = {"start_date":self.year_ago,"end_date":self.yesterday}
        return params

    def path(self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
        ) -> str:

        campaign_id = stream_slice["campaign_id"]
        try:
            return f"api/analytics/campaign_insights/{campaign_id}"
        except:
            print("No data for this campaign_id")
            return None
        
# class AnalyticsLeaderboard(SourceSosha):
        # """Currently no partners need this endpoint returned, 
        # but it would be as simple as copying all of the logic in the CampaignInsights class
        # and changing the path to path = f"api/analytics/leaderboard/{campaign_id}"

        # """
        

# Source
class SourceSosha(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        connection_url = f"https://share.climatepower.us/api/campaign_list"
        headers = {'Authorization': f"Bearer {config['api_key']}", 'Content-type': 'application/json'}
        try:
            response = requests.get(url=connection_url, headers=headers)
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e
        
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        return [Campaigns(config=config),
                CampaignInsights(config=config)
                ]
