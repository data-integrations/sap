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

package io.cdap.plugin.sap.odata.odata2;

import io.cdap.plugin.sap.odata.ODataEntity;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import java.util.Iterator;

/**
 * An iterator which iterates over every {@link ODataEntity} element, which is created from corresponding OData 2
 * {@link ODataEntry} instance of the given iterator.
 */
public class OData2EntityIterator implements Iterator<ODataEntity> {

  private final Iterator<ODataEntry> oDataEntryIterator;

  public OData2EntityIterator(Iterator<ODataEntry> oDataEntryIterator) {
    this.oDataEntryIterator = oDataEntryIterator;
  }

  @Override
  public boolean hasNext() {
    return oDataEntryIterator.hasNext();
  }

  @Override
  public ODataEntity next() {
    return ODataEntity.valueOf(oDataEntryIterator.next());
  }
}
