
import express from "express";
import { notFoundHandler } from "./middlewares/notFound.middleware.js";
import { globalErrorHandler } from "./middlewares/error.middleware.js";

const app = express();

app.use(express.json());

// Register routes
app.use("/api/users", userRoutes);
app.use("/api/posts", productRoutes);

// Handle invalid routes
app.use(notFoundHandler);

// Handle all errors
app.use(globalErrorHandler);

app.listen(3000, () => {
  console.log("Server running on port 3000");
});
