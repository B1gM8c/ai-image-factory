"use client";

import { consoleFetch } from "@/lib/auth/client";
import type {
  CreateProjectBatchRequest,
  DeletedProjectFile,
  ProjectBatch,
  ProjectBatchList,
  ProjectFile,
  ProjectFileList,
} from "@/lib/admin/types";

export class AdminApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "AdminApiError";
  }
}

export async function fetchAdminJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await consoleFetch(`/api/gateway${path}`, { signal });
  if (!response.ok) {
    throw new AdminApiError(await responseMessage(response), response.status);
  }
  return (await response.json()) as T;
}

export async function listProjectFiles(
  projectId: string,
  signal?: AbortSignal,
): Promise<ProjectFileList> {
  return projectRequest<ProjectFileList>(projectId, "/files", { signal });
}

export async function uploadProjectFile(
  projectId: string,
  file: File,
): Promise<ProjectFile> {
  const body = new FormData();
  body.set("file", file);
  body.set("purpose", "batch");
  return projectRequest<ProjectFile>(projectId, "/files", {
    method: "POST",
    body,
  });
}

export async function deleteProjectFile(
  projectId: string,
  fileId: string,
): Promise<DeletedProjectFile> {
  return projectRequest<DeletedProjectFile>(
    projectId,
    `/files/${encodeURIComponent(fileId)}`,
    { method: "DELETE" },
  );
}

export async function downloadProjectFile(
  projectId: string,
  fileId: string,
  signal?: AbortSignal,
): Promise<Blob> {
  const response = await consoleFetch(
    `/api/gateway${projectPath(projectId, `/files/${encodeURIComponent(fileId)}/content`)}`,
    { signal },
  );
  if (!response.ok) {
    throw new AdminApiError(await responseMessage(response), response.status);
  }
  return response.blob();
}

export async function listProjectBatches(
  projectId: string,
  signal?: AbortSignal,
): Promise<ProjectBatchList> {
  return projectRequest<ProjectBatchList>(projectId, "/batches", { signal });
}

export async function getProjectBatch(
  projectId: string,
  batchId: string,
  signal?: AbortSignal,
): Promise<ProjectBatch> {
  return projectRequest<ProjectBatch>(
    projectId,
    `/batches/${encodeURIComponent(batchId)}`,
    { signal },
  );
}

export async function createProjectBatch(
  projectId: string,
  request: CreateProjectBatchRequest,
): Promise<ProjectBatch> {
  return projectRequest<ProjectBatch>(projectId, "/batches", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(request),
  });
}

export async function cancelProjectBatch(
  projectId: string,
  batchId: string,
): Promise<ProjectBatch> {
  return projectRequest<ProjectBatch>(
    projectId,
    `/batches/${encodeURIComponent(batchId)}/cancel`,
    { method: "POST" },
  );
}

async function projectRequest<T>(
  projectId: string,
  suffix: string,
  init?: RequestInit,
): Promise<T> {
  const response = await consoleFetch(
    `/api/gateway${projectPath(projectId, suffix)}`,
    init,
  );
  if (!response.ok) {
    throw new AdminApiError(await responseMessage(response), response.status);
  }
  return (await response.json()) as T;
}

function projectPath(projectId: string, suffix: string) {
  return `/v1/console/projects/${encodeURIComponent(projectId)}${suffix}`;
}

async function responseMessage(response: Response) {
  try {
    const payload = (await response.json()) as {
      error?: string | { message?: string };
    };
    if (typeof payload.error === "string") return payload.error;
    if (payload.error?.message) return payload.error.message;
  } catch {
    // The status-specific fallback is safer than exposing an upstream body.
  }
  if (response.status === 403) {
    return "This account cannot view platform operations data";
  }
  if (response.status === 429) {
    return "The admin query service is busy. Please try again shortly";
  }
  return "Admin data is temporarily unavailable";
}
