require "./src/crdo/types"
require "./src/crdo/when_parser"
require "./src/crdo/config"
require "./src/crdo/task"
require "./src/crdo/task_support"
require "./src/crdo/task_state"
require "./src/crdo/schedule_support"
require "./src/crdo/schedule"
require "./src/crdo/cli"

{% unless flag?(:crdo_spec) %}
main
{% end %}
