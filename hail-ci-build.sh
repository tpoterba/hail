#!/bin/bash
set -ex

PROJECTS=$(cat projects.txt)

for project in $PROJECTS; do
    if [[ -e $project/hail-ci-build.sh ]]; then
        CHANGED=$(python3 project-changed.py target/$TARGET_BRANCH $project)
        if [[ $CHANGED != no ]]; then
            (cd $project && /bin/bash hail-ci-build.sh)
        else
            echo '<p><span style="color:gray;font-weight:bold">SKIPPED</span></p>' >> ${ARTIFACTS}/${project}
        fi
    fi
done
