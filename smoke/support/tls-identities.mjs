// Suite-owned certificate authority and broker-exclusive TLS identities.

import { chmod } from "node:fs/promises";

const KAFKA_SECRET_FILES = ["kafka-driver.p12", "key-password", "store-password"];

export async function createBrokerIdentities(t, root, brokerNames, password) {
  const authority = await t.tempDir("kafka-secure-cluster-authority");
  const authorityConfig = authority.path("authority.cnf");
  const certificate = authority.path("ca-cert.pem");
  const privateKey = authority.path("ca-key.pem");
  await t.fs.writeText(authorityConfig, certificateAuthorityConfig());
  await t.cmd(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-days",
      "2",
      "-config",
      authorityConfig,
      "-keyout",
      privateKey,
      "-out",
      certificate,
    ],
    { cwd: root, timeout: "15s" },
  );

  const identities = new Map();
  for (const [index, brokerName] of brokerNames.entries()) {
    const secrets = await createBrokerIdentity(
      t,
      root,
      authority,
      brokerName,
      index + 1,
      password,
    );
    identities.set(brokerName, secrets);
  }
  return { authority, certificate, identities };
}

async function createBrokerIdentity(t, root, authority, brokerName, serial, password) {
  const secrets = await t.tempDir(`${brokerName}-tls-identity`);
  const config = secrets.path("broker.cnf");
  const request = secrets.path("broker.csr");
  const certificate = secrets.path("broker-cert.pem");
  const privateKey = secrets.path("broker-key.pem");
  await t.fs.writeText(config, brokerCertificateConfig(brokerName));

  await t.cmd(
    "openssl",
    [
      "req",
      "-new",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-config",
      config,
      "-keyout",
      privateKey,
      "-out",
      request,
    ],
    { cwd: root, timeout: "15s" },
  );
  await t.cmd(
    "openssl",
    [
      "x509",
      "-req",
      "-in",
      request,
      "-CA",
      authority.path("ca-cert.pem"),
      "-CAkey",
      authority.path("ca-key.pem"),
      "-set_serial",
      String(serial),
      "-days",
      "2",
      "-sha256",
      "-extfile",
      config,
      "-extensions",
      "certificate_extensions",
      "-out",
      certificate,
    ],
    { cwd: root, timeout: "15s" },
  );
  await t.cmd(
    "openssl",
    [
      "pkcs12",
      "-export",
      "-in",
      certificate,
      "-inkey",
      privateKey,
      "-certfile",
      authority.path("ca-cert.pem"),
      "-out",
      secrets.path("kafka-driver.p12"),
      "-name",
      brokerName,
      "-passout",
      `pass:${password}`,
    ],
    { cwd: root, timeout: "15s" },
  );
  await t.cmd(
    "openssl",
    ["verify", "-CAfile", authority.path("ca-cert.pem"), certificate],
    { cwd: root, timeout: "10s" },
  );
  await t.cmd(
    "openssl",
    ["x509", "-in", certificate, "-noout", "-checkhost", brokerName],
    { cwd: root, timeout: "10s" },
  );
  await t.fs.writeText(secrets.path("key-password"), password);
  await t.fs.writeText(secrets.path("store-password"), password);
  await makeBrokerSecretsContainerReadable(secrets);
  return secrets;
}

export async function makeBrokerSecretsContainerReadable(secrets) {
  await chmod(secrets.toString(), 0o755);
  await Promise.all(
    KAFKA_SECRET_FILES.map((name) => chmod(secrets.path(name), 0o644)),
  );
}

function certificateAuthorityConfig() {
  return `[req]
distinguished_name = distinguished_name
prompt = no
x509_extensions = certificate_extensions

[distinguished_name]
CN = kafka-driver qualification authority

[certificate_extensions]
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
`;
}

function brokerCertificateConfig(brokerName) {
  return `[req]
distinguished_name = distinguished_name
prompt = no
req_extensions = request_extensions

[distinguished_name]
CN = ${brokerName}

[request_extensions]
subjectAltName = @subject_alt_names

[certificate_extensions]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @subject_alt_names
authorityKeyIdentifier = keyid, issuer

[subject_alt_names]
DNS.1 = ${brokerName}
`;
}
