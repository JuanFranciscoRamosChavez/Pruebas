// src/supabaseClient.js
import { createClient } from '@supabase/supabase-js'

// PEGA AQUÍ TU URL DEL PROYECTO (Ej: https://xyz.supabase.co)
const projectUrl = 'https://qkqablpmbseuhdfpamlp.supabase.co'

// PEGA AQUÍ TU CLAVE "ANON PUBLIC"
const anonKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFrcWFibHBtYnNldWhkZnBhbWxwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjM1MTM1MzUsImV4cCI6MjA3OTA4OTUzNX0.NYYyjzz3M14ERkMa42dbRwQiCW6-pZ5DNYxTuYgaigc'

export const supabase = createClient(projectUrl, anonKey)