begin;
CREATE INDEX travelTimeIdx ON history (travelTime);
end;

begin;
CREATE INDEX lastUpdatedIdx ON history (lastUpdated);
end;
