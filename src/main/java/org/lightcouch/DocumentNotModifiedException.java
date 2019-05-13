/*
 * Copyright (C) 2019 Indaba Consultores SL
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

package org.lightcouch;

/**
 * Thrown when document has not been modified
 */
public class DocumentNotModifiedException extends CouchDbException {

    private static final long serialVersionUID = 1L;

    public DocumentNotModifiedException(String message) {
        super(message);
    }

    public DocumentNotModifiedException(Throwable cause) {
        super(cause);
    }

    public DocumentNotModifiedException(String message, Throwable cause) {
        super(message, cause);
    }
}
