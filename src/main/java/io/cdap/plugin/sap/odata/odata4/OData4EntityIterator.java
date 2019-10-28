/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sap.odata.odata4;

import io.cdap.plugin.sap.odata.ODataEntity;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;

import java.util.Iterator;

/**
 * An iterator which iterates over every {@link ODataEntity} element, which is created from corresponding OData 4
 * {@link ClientEntity} instance of the given iterator.
 */
public class OData4EntityIterator implements Iterator<ODataEntity> {

  private final ClientEntitySetIterator<ClientEntitySet, ClientEntity> clientEntityIterator;

  public OData4EntityIterator(ClientEntitySetIterator<ClientEntitySet, ClientEntity> clientEntityIterator) {
    this.clientEntityIterator = clientEntityIterator;
  }

  @Override
  public boolean hasNext() {
    return clientEntityIterator.hasNext();
  }

  @Override
  public ODataEntity next() {
    return ODataEntity.valueOf(clientEntityIterator.next());
  }
}
