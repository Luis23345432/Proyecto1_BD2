"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/lib/auth-context";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function DBMSManagerPage() {
  const router = useRouter();
  const { isAuthenticated, isLoading, username, logout } = useAuth();
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);

  useEffect(() => {
    if (!isLoading && !isAuthenticated) {
      router.push("/");
    }
  }, [isAuthenticated, isLoading, router]);

  const handleLogout = () => {
    logout();
    router.push("/");
  };

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-alabaster to-timberwolf">
        <div className="text-center">
          <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-cambridge-blue"></div>
          <p className="mt-4 text-ebony font-medium">Loading...</p>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-alabaster to-timberwolf p-4">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8 pt-6">
          <div>
            <h1 className="text-3xl font-bold text-ebony">DBMS Manager</h1>
            <p className="text-davys-gray mt-1">Welcome, {username}</p>
          </div>

          <div className="relative">
            <button
              onClick={() => setIsDropdownOpen(!isDropdownOpen)}
              className="flex items-center justify-center w-10 h-10 rounded-full bg-cambridge-blue hover:bg-ebony text-alabaster transition-colors"
              aria-label="User menu"
            >
              <svg className="w-6 h-6" fill="currentColor" viewBox="0 0 24 24">
                <path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z" />
              </svg>
            </button>

            {/* Dropdown Menu */}
            {isDropdownOpen && (
              <div className="absolute right-0 mt-2 w-48 bg-alabaster border border-timberwolf rounded-lg shadow-lg z-10 overflow-hidden">
                <div className="p-3 border-b border-timberwolf bg-white rounded-t-lg">
                  <p className="text-sm font-medium text-ebony">{username}</p>
                </div>
                <button
                  onClick={handleLogout}
                  className="w-full text-left px-4 py-2 text-davys-gray bg-white hover:bg-timberwolf hover:text-ebony transition-colors text-sm font-medium rounded-b-lg"
                >
                  Logout
                </button>
              </div>
            )}
          </div>
        </div>

        {/* Main Content */}
        <Card className="shadow-lg">
          <CardHeader>
            <CardTitle className="text-ebony">
              Database Management System
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-center py-12">
              <p className="text-davys-gray text-lg">
                DBMS Manager interface coming soon...
              </p>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
