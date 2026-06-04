#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Dev-only GHA dashboard launcher with a local fake ASF session."""

import os
import sys
import types
import typing
import collections
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SERVER_DIR = ROOT_DIR / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))
os.chdir(SERVER_DIR)

import app  # noqa: E402
import asfquart.session  # noqa: E402

LOCAL_SESSION = asfquart.session.ClientSession(
    {
        "uid": "local",
        "email": "local@apache.org",
        "isRoot": True,
        "projects": ["cassandra", "mina", "polaris", "iceberg"],
    }
)


async def local_session_read(*_args, **_kwargs):
    return LOCAL_SESSION


asfquart.session.read = local_session_read

pluginEntry = collections.namedtuple("plugin", ("slug", "title", "icon", "loops", "private"))


class PluginList:
    """Minimal plugin registry that loads only the GitHub Actions endpoint/plugin."""

    def __init__(self):
        self.plugins = []

    def register(self, *loops: typing.Callable, slug: str, title: str, icon: str, private: bool = False):
        self.plugins.append(pluginEntry(slug, title, icon, loops, private or None))
        for loop in loops:
            app.asfquart.APP.add_background_task(loop)


def install_gha_only_modules():
    """Install lightweight endpoint/plugin packages before importing GHA modules."""
    endpoints = types.ModuleType("app.endpoints")
    endpoints.__path__ = [str(SERVER_DIR / "app" / "endpoints")]
    plugins = types.ModuleType("app.plugins")
    plugins.__path__ = [str(SERVER_DIR / "app" / "plugins")]
    plugins.root = PluginList()

    sys.modules["app.endpoints"] = endpoints
    sys.modules["app.plugins"] = plugins

    from app.endpoints import builds  # noqa: F401,E402
    from app.plugins import ghascanner  # noqa: F401,E402

    endpoints.builds = builds
    plugins.ghascanner = ghascanner


application = app.main(debug=True)
original_load_endpoints = application.before_serving_funcs[0]


@application.route("/auth")
async def local_auth():
    """Return the fake local session expected by the static UI OAuth gate."""
    return LOCAL_SESSION

async def load_gha_only_endpoints():
    """Load only GitHub Actions modules and static asset generation."""
    install_gha_only_modules()
    # Run the asset generation part of app.main's before_serving hook after the
    # GHA-only modules are installed, without importing every production scanner.
    from app.lib import assets  # noqa: E402
    async with application.app_context():
        application.add_background_task(assets.loop, app.STATIC_DIR, app.HTDOCS_DIR)

application.before_serving_funcs.clear()
application.before_serving(load_gha_only_endpoints)

if __name__ == "__main__":
    application.run(host=app.config.server.bind, port=app.config.server.port)
