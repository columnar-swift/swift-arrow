#!/bin/bash
git ls-files -- '*.swift' | xargs -0 swift format format --parallel --in-place
git ls-files -- '*.swift' | xargs -0 swift format lint --strict --parallel
