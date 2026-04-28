-- Adds the sampling decision captured from the origin trace so buffered-message replay through
-- TraceContextUpdater preserves it. Rows migrated from a pre-OTel jEAP version carry NULL and are treated as
-- sampled (legacy default) on replay.
ALTER TABLE sequenced_message ADD COLUMN sampled boolean;
