/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import inet.ipaddr.ipv6.IPv6Address;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.sql.utils.IPUtils;

/**
 * {@code IP_SORT_KEY(ip)} returns a 16-byte big-endian IPv6-mapped representation of an IP address
 * whose lexicographic byte order matches {@link IPUtils#compare}.
 *
 * <p>Since b95ea81ca, {@code ExprIPType} is VARCHAR-backed and IP values flow through Calcite as
 * canonical Strings. {@link org.apache.calcite.adapter.enumerable.EnumerableSort} orders the
 * resulting String column with {@link String#compareTo}, which is byte-by-byte ASCII — so {@code
 * '0.0.0.2' < '::1'} because {@code '0' (0x30) < ':' (0x3A)}, breaking IP-numeric ordering.
 *
 * <p>Wrap an IP-typed sort key in this UDF so the Sort RelNode orders by VARBINARY whose
 * lexicographic byte-order matches {@link IPUtils#compare} (IPv4 mapped onto IPv6 then
 * 16-byte-compared).
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>(IP) -&gt; VARBINARY
 * </ul>
 */
public class IpSortKeyFunction extends ImplementorUDF {
  public IpSortKeyFunction() {
    super(new IpSortKeyImplementor(), NullPolicy.STRICT);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARBINARY_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrapUDT(List.of(List.of(PPLOperandTypes.IP_UDT)));
  }

  public static class IpSortKeyImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(IpSortKeyImplementor.class, "toSortKey", translatedOperands.get(0));
    }

    /** Convert the canonical IP string to its 16-byte big-endian IPv6 form. */
    public static ByteString toSortKey(Object obj) {
      if (obj == null) {
        return null;
      }
      String text = obj.toString();
      IPv6Address v6 = toIPv6(IPUtils.toAddress(text));
      return new ByteString(v6.getBytes());
    }

    private static IPv6Address toIPv6(inet.ipaddr.IPAddress address) {
      return address instanceof inet.ipaddr.ipv4.IPv4Address v4
          ? v4.toIPv6()
          : (IPv6Address) address;
    }
  }
}
