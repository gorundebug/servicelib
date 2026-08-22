/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package operators

import (
    "github.com/gorundebug/servicelib/runtime"
    "github.com/gorundebug/servicelib/runtime/config"
    "github.com/gorundebug/servicelib/runtime/environment"
)

// streamLink delegates Stream interface methods to an embedded parent stream.
// Embed this type in link structs to avoid repeating boilerplate delegation.
type streamLink struct {
    stream runtime.Stream
}

func (s *streamLink) GetID() int {
    return s.stream.GetID()
}

func (s *streamLink) GetName() string {
    return s.stream.GetName()
}

func (s *streamLink) GetEnvironment() environment.ServiceEnvironment {
    return s.stream.GetEnvironment()
}

func (s *streamLink) GetConfig() config.StreamConfig {
    return s.stream.GetConfig()
}

func (s *streamLink) GetTransformationName() string {
    return s.stream.GetTransformationName()
}

func (s *streamLink) GetTypeName() string {
    return s.stream.GetTypeName()
}

func (s *streamLink) Build() error {
    return nil
}
