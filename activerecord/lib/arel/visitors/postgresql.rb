# frozen_string_literal: true

module Arel # :nodoc: all
  module Visitors
    class PostgreSQL < Arel::Visitors::ToSql
      private
        def visit_Arel_Nodes_Matches(o, collector)
          op = o.case_sensitive ? " LIKE " : " ILIKE "
          collector = infix_value o, collector, op
          if o.escape
            collector << " ESCAPE "
            visit o.escape, collector
          else
            collector
          end
        end

        def visit_Arel_Nodes_DoesNotMatch(o, collector)
          op = o.case_sensitive ? " NOT LIKE " : " NOT ILIKE "
          collector = infix_value o, collector, op
          if o.escape
            collector << " ESCAPE "
            visit o.escape, collector
          else
            collector
          end
        end

        def visit_Arel_Nodes_Regexp(o, collector)
          op = o.case_sensitive ? " ~ " : " ~* "
          infix_value o, collector, op
        end

        def visit_Arel_Nodes_NotRegexp(o, collector)
          op = o.case_sensitive ? " !~ " : " !~* "
          infix_value o, collector, op
        end

        def visit_Arel_Nodes_DistinctOn(o, collector)
          collector << "DISTINCT ON ( "
          visit(o.expr, collector) << " )"
        end

        def visit_Arel_Nodes_GroupingElement(o, collector)
          collector << "( "
          visit(o.expr, collector) << " )"
        end

        def visit_Arel_Nodes_Cube(o, collector)
          collector << "CUBE"
          grouping_array_or_grouping_element o, collector
        end

        def visit_Arel_Nodes_RollUp(o, collector)
          collector << "ROLLUP"
          grouping_array_or_grouping_element o, collector
        end

        def visit_Arel_Nodes_GroupingSet(o, collector)
          collector << "GROUPING SETS"
          grouping_array_or_grouping_element o, collector
        end

        def visit_Arel_Nodes_Lateral(o, collector)
          collector << "LATERAL "
          grouping_parentheses o, collector
        end

        def visit_Arel_Nodes_IsNotDistinctFrom(o, collector)
          collector = visit o.left, collector
          collector << " IS NOT DISTINCT FROM "
          visit o.right, collector
        end

        def visit_Arel_Nodes_IsDistinctFrom(o, collector)
          collector = visit o.left, collector
          collector << " IS DISTINCT FROM "
          visit o.right, collector
        end

        def visit_Arel_Nodes_NullsFirst(o, collector)
          visit o.expr, collector
          collector << " NULLS FIRST"
        end

        def visit_Arel_Nodes_NullsLast(o, collector)
          visit o.expr, collector
          collector << " NULLS LAST"
        end

        # Postgres-specific implementation that uses `col = any('{1,2}')` instead of `col IN (1,2)`
        # to avoid pg_stat_statements churn
        def visit_Arel_Nodes_HomogeneousIn(o, collector)
          oid = ActiveRecord::ConnectionAdapters::PostgreSQL::OID
          case o.attribute.type_caster
          when oid::Bytea, oid::Jsonb
            return super
          end

          visit o.left, collector
          collector << (o.type == :in ? " = any(" : " != all(")

          type_caster = oid::Array.new(o.attribute.type_caster, ",")
          values = [type_caster.serialize(o.casted_values)]
          proc_for_binds = -> value { ActiveModel::Attribute.with_cast_value(o.attribute.name, value, type_caster) }
          collector.add_binds(values, proc_for_binds, &bind_block)

          collector << ")"
        end

        BIND_BLOCK = proc { |i| "$#{i}" }
        private_constant :BIND_BLOCK

        def bind_block; BIND_BLOCK; end

        # Used by Lateral visitor to enclose select queries in parentheses
        def grouping_parentheses(o, collector)
          if o.expr.is_a? Nodes::SelectStatement
            collector << "("
            visit o.expr, collector
            collector << ")"
          else
            visit o.expr, collector
          end
        end

        # Utilized by GroupingSet, Cube & RollUp visitors to
        # handle grouping aggregation semantics
        def grouping_array_or_grouping_element(o, collector)
          if o.expr.is_a? Array
            collector << "( "
            visit o.expr, collector
            collector << " )"
          else
            visit o.expr, collector
          end
        end
    end
  end
end
