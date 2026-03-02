import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";

import providerDataRouter from "../src/routes/providerData.js";

const mockQueryData = vi.hoisted(() => vi.fn());
const mockGetProviderCities = vi.hoisted(() => vi.fn());
const mockExportProviderDataYearly = vi.hoisted(() => vi.fn());
const mockExportProviderDataMonthly = vi.hoisted(() => vi.fn());
const mockGetProviderCount = vi.hoisted(() => vi.fn());
const mockGetFlaggedCount = vi.hoisted(() => vi.fn());
const mockGetHighestRiskScore = vi.hoisted(() => vi.fn());
const mockGetProvidersWithHighRiskCount = vi.hoisted(() => vi.fn());
const mockGetProviderAnnualData = vi.hoisted(() => vi.fn());
const mockUpdateProviderDataInsights = vi.hoisted(() => vi.fn());
const mockGetProviderDataInsights = vi.hoisted(() => vi.fn());
const mockGetProviderMonthData = vi.hoisted(() => vi.fn());
const mockGetProviderDetails = vi.hoisted(() => vi.fn());
const mockAuthenticateJWT = vi.hoisted(() => vi.fn());
const mockGetLicenseCapacity = vi.hoisted(() => vi.fn());

vi.mock("../src/services/queryService.js", () => ({
  queryData: mockQueryData,
}));

vi.mock("../src/controllers/providerData.js", () => ({
  getProviderCities: mockGetProviderCities,
  exportProviderDataYearly: mockExportProviderDataYearly,
  exportProviderDataMonthly: mockExportProviderDataMonthly,
  getProviderCount: mockGetProviderCount,
  getFlaggedCount: mockGetFlaggedCount,
  getHighestRiskScore: mockGetHighestRiskScore,
  getProvidersWithHighRiskCount: mockGetProvidersWithHighRiskCount,
  getProviderAnnualData: mockGetProviderAnnualData,
  getProviderMonthData: mockGetProviderMonthData,
  getProviderDetails: mockGetProviderDetails,
  getLicenseCapacity: mockGetLicenseCapacity,
}));

vi.mock("../src/controllers/providerInsights.js", () => ({
  updateProviderDataInsights: mockUpdateProviderDataInsights,
  getProviderDataInsights: mockGetProviderDataInsights,
}));

vi.mock("../src/middlewares.js", () => ({
  authenticateJWT: mockAuthenticateJWT,
}));

describe("providerData routes", () => {
  let app: express.Application;

  beforeEach(() => {
    vi.clearAllMocks();
    app = express();
    app.use(express.json());
    app.use("/api/v1/providerData", providerDataRouter);

    mockAuthenticateJWT.mockImplementation((req, res, next) => next());
  });

  describe("GET /api/v1/providerData/cities", () => {
    it("should call getProviderCities with authentication", async () => {
      mockGetProviderCities.mockImplementation((req, res) => {
        res.json({ cities: ["New York", "Los Angeles"] });
      });

      await request(app)
        .get("/api/v1/providerData/cities/2024")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetProviderCities).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/export/year/:year", () => {
    it("should call exportProviderDataYearly with authentication", async () => {
      mockExportProviderDataYearly.mockImplementation((req, res) => {
        res.json({ year: req.params.year });
      });

      await request(app)
        .get("/api/v1/providerData/export/year/2024")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockExportProviderDataYearly).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/export/month/:month", () => {
    it("should call exportProviderDataMonthly with authentication", async () => {
      mockExportProviderDataMonthly.mockImplementation((req, res) => {
        res.json({ month: req.params.month });
      });

      await request(app)
        .get("/api/v1/providerData/export/month/2024-01")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockExportProviderDataMonthly).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/providerCount/:year", () => {
    it("should call getProviderCount with authentication", async () => {
      mockGetProviderCount.mockImplementation((req, res) => {
        res.json({ count: 100 });
      });

      await request(app)
        .get("/api/v1/providerData/providerCount/2024")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetProviderCount).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/flaggedCount/:year", () => {
    it("should call getFlaggedCount with authentication", async () => {
      mockGetFlaggedCount.mockImplementation((req, res) => {
        res.json({ count: 50 });
      });

      await request(app)
        .get("/api/v1/providerData/flaggedCount/2024")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetFlaggedCount).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/highRiskScore/:year", () => {
    it("should call getHighestRiskScore with authentication", async () => {
      mockGetHighestRiskScore.mockImplementation((req, res) => {
        res.json({ score: 95 });
      });

      await request(app)
        .get("/api/v1/providerData/highRiskScore/2024")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetHighestRiskScore).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/highRiskScoreCount/:year", () => {
    it("should call getProvidersWithHighRiskCount with authentication", async () => {
      mockGetProvidersWithHighRiskCount.mockImplementation((req, res) => {
        res.json({ count: 25 });
      });

      await request(app)
        .get("/api/v1/providerData/highRiskScoreCount/2024")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetProvidersWithHighRiskCount).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/annual/:year", () => {
    it("should call getProviderAnnualData with authentication", async () => {
      mockGetProviderAnnualData.mockImplementation((req, res) => {
        res.json({ year: req.params.year, data: [] });
      });

      await request(app)
        .get("/api/v1/providerData/annual/2024")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetProviderAnnualData).toHaveBeenCalled();
    });
  });

  describe("/api/v1/providerData/insights/:providerId", () => {
    it("should call updateProviderDataInsights for PUT requests", async () => {
      mockUpdateProviderDataInsights.mockImplementation((req, res) => {
        res.json({ success: true });
      });

      await request(app)
        .put("/api/v1/providerData/insights/123")
        .send({ insights: "test data" })
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockUpdateProviderDataInsights).toHaveBeenCalled();
    });

    it("should call getProviderDataInsights for GET requests", async () => {
      mockGetProviderDataInsights.mockImplementation((req, res) => {
        res.json({ providerId: req.params.providerId });
      });

      await request(app)
        .get("/api/v1/providerData/insights/123")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetProviderDataInsights).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/month/:month", () => {
    it("should call getProviderMonthData with authentication", async () => {
      mockGetProviderMonthData.mockImplementation((req, res) => {
        res.json({ month: req.params.month });
      });

      await request(app)
        .get("/api/v1/providerData/month/2024-01")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetProviderMonthData).toHaveBeenCalled();
    });
  });

  describe("GET /api/v1/providerData/:providerId", () => {
    it("should call getProviderDetails with authentication", async () => {
      mockGetProviderDetails.mockImplementation((req, res) => {
        res.json({ providerId: req.params.providerId });
      });

      await request(app)
        .get("/api/v1/providerData/12345")
        .expect(200);

      expect(mockAuthenticateJWT).toHaveBeenCalled();
      expect(mockGetProviderDetails).toHaveBeenCalled();
    });
  });
});
