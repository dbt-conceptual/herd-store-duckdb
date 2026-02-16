"""Tests for Entity-to-column field mapping."""

from datetime import datetime, timezone

import pytest
from herd_core.types import AgentRecord, AgentState, DecisionRecord

from herd_store_duckdb import DuckDBStoreAdapter


@pytest.fixture
def store() -> DuckDBStoreAdapter:
    """Create an in-memory DuckDB store for testing."""
    return DuckDBStoreAdapter(":memory:")


def test_agent_record_field_mapping(store: DuckDBStoreAdapter) -> None:
    """Test that AgentRecord fields map correctly to DuckDB columns."""
    agent = AgentRecord(
        id="agent-123",
        agent="mason",
        model="claude-sonnet-4-5",
        ticket_id="DBC-100",
        state=AgentState.RUNNING,
        spawned_by="steve-001",
        started_at=datetime(2026, 2, 16, 10, 0, tzinfo=timezone.utc),
        ended_at=None,
    )

    # Save
    returned_id = store.save(agent)
    assert returned_id == "agent-123"

    # Retrieve
    retrieved = store.get(AgentRecord, "agent-123")
    assert retrieved is not None
    assert retrieved.id == "agent-123"
    assert retrieved.agent == "mason"
    assert retrieved.model == "claude-sonnet-4-5"
    assert retrieved.ticket_id == "DBC-100"
    assert retrieved.state == AgentState.RUNNING
    assert retrieved.spawned_by == "steve-001"
    # DuckDB TIMESTAMP strips timezone info; compare naive datetimes
    assert retrieved.started_at == datetime(2026, 2, 16, 10, 0)
    assert retrieved.ended_at is None


def test_decision_record_field_mapping(store: DuckDBStoreAdapter) -> None:
    """Test that DecisionRecord fields map correctly to DuckDB columns."""
    decision = DecisionRecord(
        id="hdr-042",
        title="Use Protocol-Based Adapters",
        body="We will use structural subtyping for adapter interfaces",
        decision_maker="faust",
        principle="simplicity",
        scope="architecture",
        status="accepted",
    )

    # Save
    returned_id = store.save(decision)
    assert returned_id == "hdr-042"

    # Retrieve
    retrieved = store.get(DecisionRecord, "hdr-042")
    assert retrieved is not None
    assert retrieved.id == "hdr-042"
    assert retrieved.title == "Use Protocol-Based Adapters"
    assert retrieved.body == "We will use structural subtyping for adapter interfaces"
    assert retrieved.decision_maker == "faust"
    assert retrieved.principle == "simplicity"
    assert retrieved.scope == "architecture"
    assert retrieved.status == "accepted"


def test_agent_record_list_with_mapped_filters(store: DuckDBStoreAdapter) -> None:
    """Test listing AgentRecords with filters on mapped fields."""
    agent1 = AgentRecord(id="agent-1", agent="mason", model="claude-sonnet-4-5")
    agent2 = AgentRecord(id="agent-2", agent="fresco", model="claude-sonnet-4-5")
    agent3 = AgentRecord(id="agent-3", agent="mason", model="claude-opus-4-6")

    store.save(agent1)
    store.save(agent2)
    store.save(agent3)

    # Filter by agent field (maps to agent_code column)
    masons = store.list(AgentRecord, agent="mason")
    assert len(masons) == 2
    assert all(a.agent == "mason" for a in masons)

    # Filter by model field (maps to model_code column)
    sonnet_users = store.list(AgentRecord, model="claude-sonnet-4-5")
    assert len(sonnet_users) == 2
    assert all(a.model == "claude-sonnet-4-5" for a in sonnet_users)


def test_decision_record_list_with_mapped_filters(store: DuckDBStoreAdapter) -> None:
    """Test listing DecisionRecords with filters on mapped fields."""
    decision1 = DecisionRecord(
        id="hdr-1",
        title="Decision 1",
        body="Body 1",
        decision_maker="faust",
        scope="architecture",
    )
    decision2 = DecisionRecord(
        id="hdr-2",
        title="Decision 2",
        body="Body 2",
        decision_maker="steve",
        scope="process",
    )
    decision3 = DecisionRecord(
        id="hdr-3",
        title="Decision 3",
        body="Body 3",
        decision_maker="faust",
        scope="process",
    )

    store.save(decision1)
    store.save(decision2)
    store.save(decision3)

    # Filter by decision_maker field (maps to decided_by column)
    faust_decisions = store.list(DecisionRecord, decision_maker="faust")
    assert len(faust_decisions) == 2
    assert all(d.decision_maker == "faust" for d in faust_decisions)

    # Filter by scope field (maps to decision_type column)
    process_decisions = store.list(DecisionRecord, scope="process")
    assert len(process_decisions) == 2
    assert all(d.scope == "process" for d in process_decisions)
