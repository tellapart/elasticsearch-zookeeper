/*
 * Copyright 2011 Sonian Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sonian.elasticsearch.rest.zookeeper;

import com.sonian.elasticsearch.action.zookeeper.NodesZooKeeperStatusRequest;
import com.sonian.elasticsearch.action.zookeeper.NodesZooKeeperStatusResponse;
import com.sonian.elasticsearch.action.zookeeper.TransportNodesZooKeeperStatusAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

/**
 */
public class RestZooKeeperStatusAction extends BaseRestHandler {

    private final TransportNodesZooKeeperStatusAction transportNodesZooKeeperStatusAction;

    @Inject
    public RestZooKeeperStatusAction(Settings settings, Client client, RestController controller, TransportNodesZooKeeperStatusAction transportNodesZooKeeperStatusAction) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.GET, "/_zookeeper/status", this);
        controller.registerHandler(RestRequest.Method.GET, "/_zookeeper/status/{nodeId}", this);
        this.transportNodesZooKeeperStatusAction = transportNodesZooKeeperStatusAction;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        NodesZooKeeperStatusRequest zooKeeperStatusRequest = new NodesZooKeeperStatusRequest(nodesIds);
        zooKeeperStatusRequest.zooKeeperTimeout(request.paramAsTime("timeout", TimeValue.timeValueSeconds(10)));
        transportNodesZooKeeperStatusAction.execute(zooKeeperStatusRequest, new RestBuilderListener<NodesZooKeeperStatusResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesZooKeeperStatusResponse result, XContentBuilder builder) throws Exception {
                return new BytesRestResponse(RestStatus.OK, result.toXContent(builder, null));
            }
        });
    }
}
