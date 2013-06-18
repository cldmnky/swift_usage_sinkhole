#!/usr/bin/env python
# Copyright (c) 2011-2012 CloudOps
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

from setuptools import setup, find_packages
from swift_usage_sinkhole import __version__ as version

name = 'swift_usage_sinkhole'

setup(
    name=name,
    version=version,
    description='Swift middleware and server for usage logging',
    license='Apache License (2.0)',
    classifiers=['Programming Language :: Python'],
    keywords='usage swift openstack',
    author='RedBridge AB / Magnus Bengtsson (cldmnky)',
    author_email='mbengtsson@redbridge.se',
    packages=find_packages(),
    entry_points={
        'paste.filter_factory': [
            'swift_usage_sinkhole=swift_usage_sinkhole.middleware:filter_factory',
        ],
    },
)

