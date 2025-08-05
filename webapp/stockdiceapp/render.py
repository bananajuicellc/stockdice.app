# Copyright 2025 Banana Juice LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Custom rendering utilities to makes sure the desired options are set."""

import flask


def render_template(template_name_or_list, **context):
    """Renders a template but includes the canonical URL."""
    canonical_url = flask.url_for(
        flask.request.endpoint,
        _external=True,
        _scheme="https",
        **flask.request.view_args,
    )
    return flask.render_template(
        template_name_or_list, canonical_url=canonical_url, **context
    )
