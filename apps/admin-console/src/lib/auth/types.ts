export type AuthUser = {
  id: string;
  email: string;
  display_name: string;
  roles: string[];
  scopes: string[];
};

export type AuthOrganization = {
  id: string;
  name: string;
  role: string;
  is_default: boolean;
};

export type AuthProject = {
  id: string;
  organization_id: string;
  name: string;
  role: string;
  is_default: boolean;
};

export type AuthIdentity = {
  user: AuthUser;
  organizations: AuthOrganization[];
  projects: AuthProject[];
  capabilities: string[];
};

export type ConsoleSessionStatus = {
  authenticated: boolean;
  mode: "jwt" | "emergency" | "none";
  access_expires_at: number | null;
  refresh_available: boolean;
  user: AuthUser | null;
  organizations: AuthOrganization[];
  projects: AuthProject[];
  capabilities: string[];
};

export function isAuthIdentity(value: unknown): value is AuthIdentity {
  if (!isRecord(value) || !isAuthUser(value.user)) return false;
  return (
    isArrayOf(value.organizations, isAuthOrganization) &&
    isArrayOf(value.projects, isAuthProject) &&
    isArrayOf(value.capabilities, isString)
  );
}

export function parseAuthIdentity(value: unknown): AuthIdentity | null {
  if (!isRecord(value) || !isAuthUser(value.user)) return null;
  if (!isArrayOf(value.capabilities, isString)) return null;
  if (!Array.isArray(value.organizations) || !Array.isArray(value.projects)) return null;

  const organizations = value.organizations.map(parseOrganization);
  const projects = value.projects.map(parseProject);
  if (organizations.some((item) => item === null) || projects.some((item) => item === null)) {
    return null;
  }

  return {
    user: value.user,
    organizations: organizations as AuthOrganization[],
    projects: projects as AuthProject[],
    capabilities: value.capabilities,
  };
}

export function isConsoleSessionStatus(value: unknown): value is ConsoleSessionStatus {
  if (!isRecord(value)) return false;
  if (
    typeof value.authenticated !== "boolean" ||
    !["jwt", "emergency", "none"].includes(String(value.mode)) ||
    !(value.access_expires_at === null || typeof value.access_expires_at === "number") ||
    typeof value.refresh_available !== "boolean" ||
    !(value.user === null || isAuthUser(value.user))
  ) {
    return false;
  }
  return (
    isArrayOf(value.organizations, isAuthOrganization) &&
    isArrayOf(value.projects, isAuthProject) &&
    isArrayOf(value.capabilities, isString)
  );
}

function isAuthUser(value: unknown): value is AuthUser {
  return (
    isRecord(value) &&
    isString(value.id) &&
    isString(value.email) &&
    isString(value.display_name) &&
    isArrayOf(value.roles, isString) &&
    isArrayOf(value.scopes, isString)
  );
}

function isAuthOrganization(value: unknown): value is AuthOrganization {
  return (
    isRecord(value) &&
    isString(value.id) &&
    isString(value.name) &&
    isString(value.role) &&
    typeof value.is_default === "boolean"
  );
}

function isAuthProject(value: unknown): value is AuthProject {
  return (
    isRecord(value) &&
    isString(value.id) &&
    isString(value.organization_id) &&
    isString(value.name) &&
    isString(value.role) &&
    typeof value.is_default === "boolean"
  );
}

function parseOrganization(value: unknown): AuthOrganization | null {
  if (isAuthOrganization(value)) return value;
  if (
    !isRecord(value) ||
    !isString(value.organization_id) ||
    !isString(value.display_name) ||
    !isString(value.role) ||
    typeof value.is_personal !== "boolean"
  ) {
    return null;
  }
  return {
    id: value.organization_id,
    name: value.display_name,
    role: value.role,
    is_default: value.is_personal,
  };
}

function parseProject(value: unknown): AuthProject | null {
  if (isAuthProject(value)) return value;
  if (
    !isRecord(value) ||
    !isString(value.project_id) ||
    !isString(value.organization_id) ||
    !isString(value.display_name) ||
    !isString(value.role) ||
    typeof value.is_default !== "boolean"
  ) {
    return null;
  }
  return {
    id: value.project_id,
    organization_id: value.organization_id,
    name: value.display_name,
    role: value.role,
    is_default: value.is_default,
  };
}

function isArrayOf<T>(
  value: unknown,
  predicate: (item: unknown) => item is T,
): value is T[] {
  return Array.isArray(value) && value.every(predicate);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object" && !Array.isArray(value));
}
