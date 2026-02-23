/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.routing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;
import org.opensearch.dqe.plugin.settings.DqeSettings;

@ExtendWith(MockitoExtension.class)
class EngineRouterTests {

  @Mock private DqeSettings settings;

  private EngineRouter router;

  @BeforeEach
  void setUp() {
    // Orchestrator and explain handler are null (PL-10/PL-5 not yet implemented)
    router = new EngineRouter(settings, null, null);
  }

  @Nested
  @DisplayName("resolveEngine()")
  class ResolveEngineTests {

    @Test
    @DisplayName("returns request engine field when present")
    void requestFieldTakesPrecedence() {
      assertEquals("dqe", router.resolveEngine("dqe"));
    }

    @Test
    @DisplayName("returns calcite when request specifies calcite")
    void requestFieldCalcite() {
      assertEquals("calcite", router.resolveEngine("calcite"));
    }

    @Test
    @DisplayName("normalizes engine field to lowercase")
    void normalizesToLowerCase() {
      assertEquals("dqe", router.resolveEngine("DQE"));
      assertEquals("calcite", router.resolveEngine("CALCITE"));
    }

    @Test
    @DisplayName("falls back to cluster setting when request field is null")
    void fallsBackToClusterSetting() {
      when(settings.getEngine()).thenReturn("dqe");
      assertEquals("dqe", router.resolveEngine(null));
    }

    @Test
    @DisplayName("falls back to cluster setting when request field is empty")
    void fallsBackOnEmptyField() {
      when(settings.getEngine()).thenReturn("calcite");
      assertEquals("calcite", router.resolveEngine(""));
    }

    @Test
    @DisplayName("throws INVALID_REQUEST for invalid engine value")
    void rejectsInvalidEngine() {
      DqeException ex = assertThrows(DqeException.class, () -> router.resolveEngine("spark"));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }
  }

  @Nested
  @DisplayName("shouldUseDqe()")
  class ShouldUseDqeTests {

    @Test
    @DisplayName("returns true when engine=dqe and DQE is enabled")
    void trueWhenDqeEnabled() {
      when(settings.isDqeEnabled()).thenReturn(true);
      assertTrue(router.shouldUseDqe("dqe"));
    }

    @Test
    @DisplayName("throws DQE_DISABLED when engine=dqe but DQE is disabled")
    void throwsWhenDqeDisabled() {
      when(settings.isDqeEnabled()).thenReturn(false);
      DqeException ex = assertThrows(DqeException.class, () -> router.shouldUseDqe("dqe"));
      assertEquals(DqeErrorCode.DQE_DISABLED, ex.getErrorCode());
    }

    @Test
    @DisplayName("returns false when engine=calcite")
    void falseForCalcite() {
      assertFalse(router.shouldUseDqe("calcite"));
    }

    @Test
    @DisplayName("returns false when cluster default is calcite and no request override")
    void falseForDefaultCalcite() {
      when(settings.getEngine()).thenReturn("calcite");
      assertFalse(router.shouldUseDqe(null));
    }

    @Test
    @DisplayName("returns true when cluster default is dqe and no request override")
    void trueForDefaultDqe() {
      when(settings.getEngine()).thenReturn("dqe");
      when(settings.isDqeEnabled()).thenReturn(true);
      assertTrue(router.shouldUseDqe(null));
    }

    @Test
    @DisplayName("request engine field overrides cluster default")
    void requestOverridesCluster() {
      // Request says calcite, should return false even if cluster default would be dqe.
      // resolveEngine("calcite") returns early without calling settings.getEngine().
      assertFalse(router.shouldUseDqe("calcite"));
    }

    @Test
    @DisplayName("request engine=dqe overrides calcite cluster default")
    void requestDqeOverridesCalciteDefault() {
      // resolveEngine("dqe") returns early without calling settings.getEngine().
      when(settings.isDqeEnabled()).thenReturn(true);
      assertTrue(router.shouldUseDqe("dqe"));
    }
  }

  @Nested
  @DisplayName("executeQuery()")
  class ExecuteQueryTests {

    @Test
    @DisplayName("throws UNSUPPORTED_OPERATION when orchestrator is null")
    void throwsWhenOrchestratorNull() {
      DqeQueryRequest request = new DqeQueryRequest("SELECT 1", "dqe");
      DqeException ex = assertThrows(DqeException.class, () -> router.executeQuery(request));
      assertEquals(DqeErrorCode.UNSUPPORTED_OPERATION, ex.getErrorCode());
    }
  }

  @Nested
  @DisplayName("executeExplain()")
  class ExecuteExplainTests {

    @Test
    @DisplayName("throws UNSUPPORTED_OPERATION when explain handler is null")
    void throwsWhenExplainHandlerNull() {
      DqeQueryRequest request = new DqeQueryRequest("SELECT 1", "dqe");
      DqeException ex = assertThrows(DqeException.class, () -> router.executeExplain(request));
      assertEquals(DqeErrorCode.UNSUPPORTED_OPERATION, ex.getErrorCode());
    }
  }
}
