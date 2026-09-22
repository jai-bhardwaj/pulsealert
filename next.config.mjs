/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: { serverComponentsExternalPackages: ["kafkajs"] },
};
export default nextConfig;
