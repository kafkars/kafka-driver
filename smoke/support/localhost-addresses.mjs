// Host-resolver precondition for an IPv6 refusal followed by live IPv4 Kafka.

import { lookup } from "node:dns/promises";

export async function requireDeadFirstLocalhost(port) {
  const addresses = await lookup("localhost", { all: true, verbatim: true });
  const first = addresses.at(0);
  const liveIndex = addresses.findIndex(
    ({ address, family }, index) => index > 0 && address === "127.0.0.1" && family === 4,
  );
  if (first?.address !== "::1" || first.family !== 6 || liveIndex < 1) {
    const observed = addresses.map(({ address, family }) => `${family}:${address}`).join(",");
    throw new Error(
      `qualification requires IPv6 ::1 before IPv4 127.0.0.1; observed ${observed}`,
    );
  }
  return `localhost:${port}`;
}
