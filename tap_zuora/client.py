from typing import Dict, Tuple
import pendulum
import backoff
import requests
import singer
from singer import metrics

from tap_zuora.exceptions import (
    ApiException,
    BadCredentialsException,
    RateLimitException,
    RetryableException,
)
from tap_zuora.utils import make_aqua_payload

IS_PROD = False
IS_SAND = True

URLS = {
    IS_PROD: "https://rest.na.zuora.com/",
    IS_SAND: "https://rest.sandbox.na.zuora.com/",
}

LATEST_WSDL_VERSION = "91.0"

LOGGER = singer.get_logger()


class Client:
    """
    A client class for interacting with Zuora API.

    Attributes:
        client_id (str): The client ID for authentication.
        client_secret (str): The client secret for authentication.
        sandbox (bool): Flag indicating whether to use sandbox environment.
        partner_id (str): The partner ID.
        is_rest (bool): Flag indicating whether to use REST API.
        _session (requests.Session): Session object for making HTTP requests.
        oauth2_token (dict): OAuth2 token for authentication.
        token_expiration_date (pendulum.DateTime): Expiration date of OAuth2 token.
        base_url (str): Base URL for API requests.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        partner_id: str,
        sandbox: bool = False,
        is_rest: bool = False,
    ):
        """
        Initialize the Client object.

        Args:
            client_id (str): The client ID for authentication.
            client_secret (str): The client secret for authentication.
            partner_id (str): The partner ID.
            sandbox (bool, optional): Flag indicating whether to use sandbox environment. Defaults to False.
            is_rest (bool, optional): Flag indicating whether to use REST API. Defaults to False.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.sandbox = sandbox
        self.partner_id = partner_id
        self.is_rest = is_rest
        self.oauth2_token = None
        self.token_expiration_date = None
        self.base_url = URLS[self.sandbox]
        self._session = requests.Session()

        self.check_auth()

        adapter = requests.adapters.HTTPAdapter(
            max_retries=5
        )  # Try again in the case the TCP socket closes
        self._session.mount("https://", adapter)

    @staticmethod
    def from_config(config: Dict) -> "Client":
        """
        Create a Client object from configuration.

        Args:
            config (Dict): Configuration parameters.

        Returns:
            Client: The Client object.
        """
        sandbox = config.get("sandbox", False) == "true"
        partner_id = config.get("partner_id", None)
        is_rest = config.get("api_type") == "REST"
        return Client(
            config["client_id"],
            config["client_secret"],
            partner_id,
            sandbox,
            is_rest,
        )

    def check_auth(self) -> None:
        """
        Check authentication based on given credentials.
        """
        stream_name = "Account"
        if self.is_rest:
            resp = self._retryable_request(
                "GET",
                f"{self.base_url}v1/describe/{stream_name}",
                url_check=True,
                headers=self.rest_headers,
            )

        else:
            query = f"select * from {stream_name} limit 1"
            post_url = f"{self.base_url}v1/batch-query/"
            payload = make_aqua_payload("discover", query, self.partner_id)
            resp = self._retryable_request(
                "POST", post_url, url_check=True, auth=self.aqua_auth, json=payload
            )
            if resp.status_code == 200:
                resp_json = resp.json()
                if "errorCode" in resp_json:
                    # Zuora sends 200 status code for an unrecognized partner ID in AQuA calls.
                    raise Exception(
                        resp_json.get(
                            "message",
                            "Partner ID is not recognized."
                            " To obtain a partner ID,"
                            " submit a request with Zuora Global Support",
                        )
                    )

                delete_id = resp_json["id"]
                delete_url = f"{url_prefix}v1/batch-query/jobs/{delete_id}"
                self._retryable_request("DELETE", delete_url, auth=self.aqua_auth)

        if resp.status_code == 401:
            raise BadCredentialsException(
                f"Couldnt verify given credentials for base url: {self.base_url}"
            )

    @property
    def aqua_auth(self) -> Tuple[str, str]:
        """
        Returns authentication credentials for AQuA requests.

        Returns:
            Tuple[str, str]: Username and password.
        """
        return self.client_id, self.client_secret

    @property
    def rest_headers(self) -> Dict[str, str]:
        """
        Returns headers for HTTP request.

        Returns:
            Dict[str, str]: HTTP headers.
        """
        self.ensure_valid_auth_token()
        return {
            "Authorization": "Bearer " + self.oauth2_token["access_token"],
            "Content-Type": "application/json",
        }

    # NB> Backoff as recommended by Zuora here:
    # https://community.zuora.com/t5/Release-Notifications/Upcoming-Change-for-AQuA-and-Data-Source-Export-January-2021/ba-p/35024
    @backoff.on_exception(
        backoff.expo,
        (RateLimitException, RetryableException),
        max_tries=5,
        factor=30,
        jitter=None,
    )
    def _retryable_request(
        self, method: str, url: str, stream=False, url_check=False, **kwargs
    ) -> requests.Response:
        """
        Performs HTTP request with retries.

        Args:
            method (str): HTTP method.
            url (str): URL for the request.
            stream (bool, optional): Whether to stream the response. Defaults to False.
            url_check (bool, optional): Whether the request is for URL validation. Defaults to False.

        Returns:
            requests.Response: HTTP response.
        """
        req = requests.Request(method, url, **kwargs).prepare()
        resp = self._session.send(req, stream=stream)

        if resp.status_code == 429:
            raise RateLimitException(resp)
        # retries the request when response is either 500(Internal Server Error)
        # 502(Bad Gateway), 503(service unavailable), 504(Gateway Timeout)
        if resp.status_code in [500, 502, 503, 504]:
            raise RetryableException(resp)
        self.check_for_error(resp, url_check)
        return resp

    @staticmethod
    def check_for_error(resp: requests.Response, url_check: bool) -> None:
        """
        Checks for errors in the HTTP response.

        Args:
            resp (requests.Response): HTTP response.
            url_check (bool): Whether the request is for URL validation.
        """
        if (
            not url_check
            and resp.status_code == 400
            and "noSuchDataSource"
            in resp.json().get("Errors", [{"Message": ""}])[0]["Message"]
        ):
            return

        if not url_check:
            resp.raise_for_status()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Makes an HTTP request.

        Args:
            method (str): HTTP method.
            url (str): URL for the request.

        Returns:
            requests.Response: HTTP response.
        """
        LOGGER.info(f"{method}: {url}")
        resp = self._retryable_request(method, url, **kwargs)

        if resp.status_code != 200:
            raise ApiException(resp)

        return resp

    def is_auth_token_valid(self) -> bool:
        """
        Checks if the OAuth2 token is still valid.

        Returns:
            bool: True if token is valid, False otherwise.
        """
        if (
            self.oauth2_token
            and self.token_expiration_date
            and pendulum.utcnow().diff(self.token_expiration_date).in_seconds() > 60
        ):
            return True
        return False

    def ensure_valid_auth_token(self) -> None:
        """Ensures the OAuth2 token is valid."""
        if not self.is_auth_token_valid():
            self.oauth2_token = self.request_token()

    def request_token(self) -> dict:
        """
        Requests a new OAuth2 token.

        Returns:
            dict: OAuth2 token.
        """
        url = self.base_url + "oauth/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        token = self._request("POST", url, data=payload).json()
        self.token_expiration_date = pendulum.utcnow().add(seconds=token["expires_in"])

        return token

    def aqua_request(self, method: str, path: str, **kwargs) -> requests.Response:
        """
        Makes an AQuA request.

        Args:
            method (str): HTTP method.
            path (str): API endpoint.

        Returns:
            requests.Response: HTTP response.
        """
        self.ensure_valid_auth_token()

        with metrics.http_request_timer(path):
            url = self.base_url + path
            return self._request(method, url, auth=self.aqua_auth, **kwargs)

    def rest_request(self, method: str, path: str, **kwargs) -> requests.Response:
        """
        Makes a REST API request.

        Args:
            method (str): HTTP method.
            path (str): API endpoint.

        Returns:
            requests.Response: HTTP response.
        """
        self.ensure_valid_auth_token()

        with metrics.http_request_timer(path):
            url = self.base_url + path
            return self._request(method, url, headers=self.rest_headers, **kwargs)
