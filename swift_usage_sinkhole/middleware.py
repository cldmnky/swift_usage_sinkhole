# Copyright (c) 2013 RedBridge AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import time
from urllib import quote, unquote

from swift.common.swob import Request
from swift.common.utils import (get_logger, get_remote_client,
                                get_valid_utf8_str, config_true_value,
                                InputProxy)

QUOTE_SAFE = '/:'

class UsageSinkholeMiddleware(object):
    """
    Middleware that transfer accounting data to a zmq queue.
    """
    def __init__(self, app, conf):
        self.app = app
        self.log_hdrs = config_true_value(conf.get(
            'access_log_headers',
            conf.get('log_headers', 'yes')))

    def method_from_req(self, req):
        return req.environ.get('swift.orig_req_method', req.method)

    def req_already_accounted(self, req):
        return req.environ.get('swift.usage_accounting_made')

    def mark_req_accounted(self, req):
        req.environ['swift.usage_accounting_made'] = True

    def usage_request(self, req, status_int, bytes_sent):
        """
        Send the bytes_sent to a zmq queue
        """
        if self.req_already_accounted:
            return
        req_path = get_valid_utf8_str(req.path)
        the_request = quote(unquote(req_path), QUOTE_SAFE)
        if self.log_hdrs:
            logged_headers = '\n'.join('%s: %s' % (k, v) for k, v in req.headers.items())
        method = self.method_from_req(req)
        if not req.environ.get('swift.source'):
            accounting_data = {
                        'remote_address': req.remote_addr,
                        'method': method,
                        'bytes_sent': bytes_sent
                        'headers': logged_headers,
                        'request': the_request
                    }
            print accounting_data
            self.mark_req_accounted(req)


        def __call__(self, env, start_response):
            start_response_args = [None]
            input_proxy = InputProxy(env['wsgi.input'])
            env['wsgi.input'] = input_proxy
            
            def my_start_response(status, headers, exc_info=None):
                start_response_args[0] = (status, list(headers), exc_info)

        def status_int_for_logging(client_disconnect=False, start_status=None):
            # log disconnected clients as '499' status code
            if client_disconnect or input_proxy.client_disconnect:
                return 499
            elif start_status is None:
                return int(start_response_args[0][0].split(' ', 1)[0])
            return start_status

        def iter_response(iterable):
            iterator = iter(iterable)
            try:
                chunk = iterator.next()
                while not chunk:
                    chunk = iterator.next()
            except StopIteration:
                chunk = ''
            for h, v in start_response_args[0][1]:
                if h.lower() in ('content-length', 'transfer-encoding'):
                    break
            else:
                if not chunk:
                    start_response_args[0][1].append(('content-length', '0'))
                elif isinstance(iterable, list):
                    start_response_args[0][1].append(
                        ('content-length', str(sum(len(i) for i in iterable))))
            start_response(*start_response_args[0])
            req = Request(env)

            bytes_sent = 0
            client_disconnect = False
            try:
                while chunk:
                    bytes_sent += len(chunk)
                    yield chunk
                    chunk = iterator.next()
            except GeneratorExit:  # generator was closed before we finished
                client_disconnect = True
                raise
            finally:
                status_int = status_int_for_logging(client_disconnect)
                self.log_request(
                    req, status_int, bytes_sent)

        try:
            iterable = self.app(env, my_start_response)
        except Exception:
            req = Request(env)
            status_int = status_int_for_logging(start_status=500)
            self.log_request(
                req, status_int, 0)
            raise
        else:
            return iter_response(iterable)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def usage_sinkhole(app):
        return UsageSinkholeMiddleware(app, conf)
    return usage_sinkhole
