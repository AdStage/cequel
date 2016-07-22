# -*- encoding : utf-8 -*-
begin
  require 'new_relic/agent/method_tracer'
  require 'new_relic/helper'
rescue LoadError => e
  fail LoadError, "Can't use NewRelic instrumentation without NewRelic gem"
end

# encoding: utf-8
# This file is distributed under New Relic's license terms.
# See https://github.com/newrelic/rpm/blob/master/LICENSE for complete details.
module NewRelic
  module Agent
    module Instrumentation
      module CequelHelper
        module_function

        def metric_for_sql(sql) #THREAD_LOCAL_ACCESS
          operation = NewRelic::Agent::Database.parse_operation_from_query(sql)
          if operation
            # Could not determine the model/operation so use a fallback metric
            "Database/CQL/#{operation}"
          else
            "Database/CQL/other"
          end
        end

        # Given a metric name such as "ActiveRecord/model/action" this
        # returns an array of rollup metrics:
        # [ "Datastore/all", "ActiveRecord/all", "ActiveRecord/action" ]
        # If the metric name is in the form of "ActiveRecord/action"
        # this returns merely: [ "Datastore/all", "ActiveRecord/all" ]
        def rollup_metrics_for(metric)
          metrics = ["Datastore/all"]

          # If we're outside of a web transaction, don't record any rollup
          # database metrics. This is to prevent metrics from background tasks
          # from polluting the metrics used to drive overview graphs.
          unless NewRelic::Agent::Transaction.recording_web_transaction?
            metrics << "Datastore/allOther"
          end
          metrics
        end
      end
      module Cequel
        EXPLAINER = lambda do |config, query|
          "No explain plans support for CQL currently"
        end

        def self.insert_instrumentation
          ::Cequel::Metal::RequestLogger.module_eval do
            include ::NewRelic::Agent::Instrumentation::Cequel
          end
        end

        def self.included(instrumented_class)
          instrumented_class.class_eval do
            unless instrumented_class.method_defined?(:log_without_newrelic_instrumentation)
              alias_method :log_without_newrelic_instrumentation, :log
              alias_method :log, :log_with_newrelic_instrumentation
              protected :log
            end
          end
        end

        def log_with_newrelic_instrumentation(*args, &block) #THREAD_LOCAL_ACCESS
          state = NewRelic::Agent::TransactionState.tl_get

          if !state.is_execution_traced?
            return log_without_newrelic_instrumentation(*args, &block)
          end

          name, cql, _ = args
          metric = CequelHelper.metric_for_sql(NewRelic::Helper.correctly_encoded(cql))

          if !metric
            log_without_newrelic_instrumentation(*args, &block)
          else
            metrics = [metric].compact
            metrics += CequelHelper.rollup_metrics_for(metric)
            self.class.trace_execution_scoped(metrics) do
              t0 = Time.now
              begin
                log_without_newrelic_instrumentation(*args, &block)
              ensure
                elapsed_time = (Time.now - t0).to_f

                NewRelic::Agent.instance.transaction_sampler.notice_sql(cql,
                                                      {}, elapsed_time,
                                                      state, &EXPLAINER)
                NewRelic::Agent.instance.sql_sampler.notice_sql(cql, metric,
                                                      {}, elapsed_time,
                                                      state, &EXPLAINER)
              end
            end
          end
        end

        add_method_tracer :execute_with_consistency,
                          'Database/Cassandra/#{args[0][/^[A-Z ]*[A-Z]/]' \
                          '.sub(/ FROM$/, \'\')}'
      end
    end
  end
end

DependencyDetection.defer do
  @name = :cequel

  depends_on do
    defined?(::Cequel)
  end

  depends_on do
    !NewRelic::Agent.config[:disable_cequel_instrumentation] &&
      !NewRelic::Agent.config[:disable_database_instrumentation]
  end

  executes do
    ::NewRelic::Agent.logger.info 'Installing Cequel instrumentation'
  end

  executes do
    ::NewRelic::Agent::Instrumentation::Cequel.insert_instrumentation
  end
end
