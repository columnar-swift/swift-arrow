#!/bin/bash
git ls-files -- '*.swift' ':(exclude)Sources/Arrow/Generated/*' | xargs -0 swift format format --parallel --in-place
git ls-files -- '*.swift' ':(exclude)Sources/Arrow/Generated/*' | xargs -0 swift format lint --strict --parallel
