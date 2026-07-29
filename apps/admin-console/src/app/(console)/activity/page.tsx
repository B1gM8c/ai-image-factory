import { AdminJobs } from "@/components/admin-jobs";

type ActivityPageProps = {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
};

export default async function ActivityPage({
  searchParams,
}: ActivityPageProps) {
  return <AdminJobs initialSearchParams={await searchParams} />;
}
